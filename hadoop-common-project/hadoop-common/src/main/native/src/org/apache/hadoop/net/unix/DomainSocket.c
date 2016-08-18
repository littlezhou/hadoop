/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "config.h"
#include "exception.h"
#include "org/apache/hadoop/io/nativeio/file_descriptor.h"
#include "org_apache_hadoop.h"
#include "org_apache_hadoop_net_unix_DomainSocket.h"

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <jni.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h> /* for FIONREAD */
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

//#include <libaio.h>
#include <malloc.h>
#include <fcntl.h>
#include <inttypes.h>
#include <linux/aio_abi.h>
#include <errno.h>

#define SEND_BUFFER_SIZE org_apache_hadoop_net_unix_DomainSocket_SEND_BUFFER_SIZE
#define RECEIVE_BUFFER_SIZE org_apache_hadoop_net_unix_DomainSocket_RECEIVE_BUFFER_SIZE
#define SEND_TIMEOUT org_apache_hadoop_net_unix_DomainSocket_SEND_TIMEOUT
#define RECEIVE_TIMEOUT org_apache_hadoop_net_unix_DomainSocket_RECEIVE_TIMEOUT

#define DEFAULT_RECEIVE_TIMEOUT 120000
#define DEFAULT_SEND_TIMEOUT 120000
#define LISTEN_BACKLOG 128

/* In Linux, you can pass the MSG_NOSIGNAL flag to send, sendto, etc. to prevent
 * those functions from generating SIGPIPE.  HDFS-4831 for details.
 */
#ifdef MSG_NOSIGNAL
#define PLATFORM_SEND_FLAGS MSG_NOSIGNAL
#else
#define PLATFORM_SEND_FLAGS 0
#endif

/**
 * Can't pass more than this number of file descriptors in a single message.
 */
#define MAX_PASSED_FDS 16

static jthrowable setAttribute0(JNIEnv *env, jint fd, jint type, jint val);

/**
 * Convert an errno to a socket exception name.
 *
 * Note: we assume that all of these exceptions have a one-argument constructor
 * that takes a string.
 *
 * @return               The exception class name
 */
static const char *errnoToSocketExceptionName(int errnum)
{
  switch (errnum) {
  case EAGAIN:
    /* accept(2) returns EAGAIN when a socket timeout has been set, and that
     * timeout elapses without an incoming connection.  This error code is also
     * used in non-blocking I/O, but we don't support that. */
  case ETIMEDOUT:
    return "java/net/SocketTimeoutException";
  case EHOSTDOWN:
  case EHOSTUNREACH:
  case ECONNREFUSED:
    return "java/net/NoRouteToHostException";
  case ENOTSUP:
    return "java/lang/UnsupportedOperationException";
  default:
    return "java/net/SocketException";
  }
}

static jthrowable newSocketException(JNIEnv *env, int errnum,
                                     const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));

static jthrowable newSocketException(JNIEnv *env, int errnum,
                                     const char *fmt, ...)
{
  va_list ap;
  jthrowable jthr;

  va_start(ap, fmt);
  jthr = newExceptionV(env, errnoToSocketExceptionName(errnum), fmt, ap);
  va_end(ap);
  return jthr;
}

/**
 * Flexible buffer that will try to fit data on the stack, and fall back
 * to the heap if necessary.
 */
struct flexibleBuffer {
  int8_t *curBuf;
  int8_t *allocBuf;
  int8_t stackBuf[8196];
};

static jthrowable flexBufInit(JNIEnv *env, struct flexibleBuffer *flexBuf, jint length)
{
  flexBuf->curBuf = flexBuf->allocBuf = NULL;
  if (length < sizeof(flexBuf->stackBuf)) {
    flexBuf->curBuf = flexBuf->stackBuf;
    return NULL;
  }
  flexBuf->allocBuf = malloc(length);
  if (!flexBuf->allocBuf) {
    return newException(env, "java/lang/OutOfMemoryError",
        "OOM allocating space for %d bytes of data.", length);
  }
  flexBuf->curBuf = flexBuf->allocBuf;
  return NULL;
}

static void flexBufFree(struct flexibleBuffer *flexBuf)
{
  free(flexBuf->allocBuf);
}

static jthrowable setup(JNIEnv *env, int *ofd, jobject jpath, int doConnect)
{
  const char *cpath = NULL;
  struct sockaddr_un addr;
  jthrowable jthr = NULL;
  int fd = -1, ret;

  fd = socket(PF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    ret = errno;
    jthr = newSocketException(env, ret,
        "error creating UNIX domain socket with SOCK_STREAM: %s",
        terror(ret));
    goto done;
  }
  memset(&addr, 0, sizeof(&addr));
  addr.sun_family = AF_UNIX;
  cpath = (*env)->GetStringUTFChars(env, jpath, NULL);
  if (!cpath) {
    jthr = (*env)->ExceptionOccurred(env);
    (*env)->ExceptionClear(env);
    goto done;
  }
  ret = snprintf(addr.sun_path, sizeof(addr.sun_path),
                 "%s", cpath);
  if (ret < 0) {
    ret = errno;
    jthr = newSocketException(env, EIO,
        "error computing UNIX domain socket path: error %d (%s)",
        ret, terror(ret));
    goto done;
  }
  if (ret >= sizeof(addr.sun_path)) {
    jthr = newSocketException(env, ENAMETOOLONG,
        "error computing UNIX domain socket path: path too long.  "
        "The longest UNIX domain socket path possible on this host "
        "is %zd bytes.", sizeof(addr.sun_path) - 1);
    goto done;
  }
#ifdef SO_NOSIGPIPE
  /* On MacOS and some BSDs, SO_NOSIGPIPE will keep send and sendto from causing
   * EPIPE.  Note: this will NOT help when using write or writev, only with
   * send, sendto, sendmsg, etc.  See HDFS-4831.
   */
  ret = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, (void *)&ret, sizeof(ret))) {
    ret = errno;
    jthr = newSocketException(env, ret,
        "error setting SO_NOSIGPIPE on socket: error %s", terror(ret));
    goto done;
  }
#endif
  if (doConnect) {
    RETRY_ON_EINTR(ret, connect(fd,
        (struct sockaddr*)&addr, sizeof(addr)));
    if (ret < 0) {
      ret = errno;
      jthr = newException(env, "java/net/ConnectException",
              "connect(2) error: %s when trying to connect to '%s'",
              terror(ret), addr.sun_path);
      goto done;
    }
  } else {
    RETRY_ON_EINTR(ret, unlink(addr.sun_path));
    RETRY_ON_EINTR(ret, bind(fd, (struct sockaddr*)&addr, sizeof(addr)));
    if (ret < 0) {
      ret = errno;
      jthr = newException(env, "java/net/BindException",
              "bind(2) error: %s when trying to bind to '%s'",
              terror(ret), addr.sun_path);
      goto done;
    }
    /* We need to make the socket readable and writable for all users in the
     * system.
     *
     * If the system administrator doesn't want the socket to be accessible to
     * all users, he can simply adjust the +x permissions on one of the socket's
     * parent directories.
     *
     * See HDFS-4485 for more discussion.
     */
    if (chmod(addr.sun_path, 0666)) {
      ret = errno;
      jthr = newException(env, "java/net/BindException",
              "chmod(%s, 0666) failed: %s", addr.sun_path, terror(ret));
      goto done;
    }
    if (listen(fd, LISTEN_BACKLOG) < 0) {
      ret = errno;
      jthr = newException(env, "java/net/BindException",
              "listen(2) error: %s when trying to listen to '%s'",
              terror(ret), addr.sun_path);
      goto done;
    }
  }

done:
  if (cpath) {
    (*env)->ReleaseStringUTFChars(env, jpath, cpath);
  }
  if (jthr) {
    if (fd > 0) {
      RETRY_ON_EINTR(ret, close(fd));
      fd = -1;
    }
  } else {
    *ofd = fd;
  }
  return jthr;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_anchorNative(
JNIEnv *env, jclass clazz)
{
  fd_init(env); // for fd_get, fd_create, etc.
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_validateSocketPathSecurity0(
JNIEnv *env, jclass clazz, jobject jstr, jint skipComponents)
{
  jint utfLength;
  char path[PATH_MAX], check[PATH_MAX], *token, *rest;
  struct stat st;
  int ret, mode, strlenPath;
  uid_t uid;
  jthrowable jthr = NULL;

  utfLength = (*env)->GetStringUTFLength(env, jstr);
  if (utfLength > (sizeof(path)-1)) {
    jthr = newIOException(env, "path is too long!  We expected a path "
        "no longer than %zd UTF-8 bytes.", (sizeof(path)-1));
    goto done;
  }
  (*env)->GetStringUTFRegion(env, jstr, 0, utfLength, path);
  path [ utfLength ] = 0;
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->ExceptionClear(env);
    goto done;
  }
  uid = geteuid();
  strlenPath = strlen(path);
  if (strlenPath == 0) {
    jthr = newIOException(env, "socket path is empty.");
    goto done;
  }
  if (path[strlenPath - 1] == '/') {
    /* It makes no sense to have a socket path that ends in a slash, since
     * sockets are not directories. */
    jthr = newIOException(env, "bad socket path '%s'.  The socket path "
          "must not end in a slash.", path);
    goto done;
  }
  // This loop iterates through all of the path components except for the very
  // last one.  We don't validate the last component, since it's not supposed to
  // be a directory.  (If it is a directory, we will fail to create the socket
  // later with EISDIR or similar.)
  for (check[0] = '/', check[1] = '\0', rest = path, token = "";
       token && rest && rest[0];
       token = strtok_r(rest, "/", &rest)) {
    if (strcmp(check, "/") != 0) {
      // If the previous directory we checked was '/', we skip appending another
      // slash to the end because it would be unncessary.  Otherwise we do it.
      strcat(check, "/");
    }
    // These strcats are safe because the length of 'check' is the same as the
    // length of 'path' and we never add more slashes than were in the original
    // path.
    strcat(check, token);
    if (skipComponents > 0) {
      skipComponents--;
      continue;
    }
    if (stat(check, &st) < 0) {
      ret = errno;
      jthr = newIOException(env, "failed to stat a path component: '%s'.  "
          "error code %d (%s)", check, ret, terror(ret));
      goto done;
    }
    mode = st.st_mode & 0777;
    if (mode & 0002) {
      jthr = newIOException(env, "the path component: '%s' is "
        "world-writable.  Its permissions are 0%03o.  Please fix "
        "this or select a different socket path.", check, mode);
      goto done;
    }
    if ((mode & 0020) && (st.st_gid != 0)) {
      jthr = newIOException(env, "the path component: '%s' is "
        "group-writable, and the group is not root.  Its permissions are "
        "0%03o, and it is owned by gid %d.  Please fix this or "
        "select a different socket path.", check, mode, st.st_gid);
      goto done;
    }
    if ((mode & 0200) && (st.st_uid != 0) &&
        (st.st_uid != uid)) {
      jthr = newIOException(env, "the path component: '%s' is "
        "owned by a user who is not root and not you.  Your effective user "
        "id is %d; the path is owned by user id %d, and its permissions are "
        "0%03o.  Please fix this or select a different socket path.",
        check, uid, st.st_uid, mode);
        goto done;
      goto done;
    }
  }
done:
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_bind0(
JNIEnv *env, jclass clazz, jstring path)
{
  int fd;
  jthrowable jthr = NULL;

  jthr = setup(env, &fd, path, 0);
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
  return fd;
}

#define BLOCK_SIZE 4096

struct op_info {
	int 			fd;
	int 			bufsize;

	int 			bufwritten;
	char *			bufwriting;

	aio_context_t 	ctx;
	size_t 			offset;

	int				inflying;
	int 			incache;
	int	*			freeindexs;
	int 			nbuffers;
	char**			buffers;

	struct io_event * events;
	struct iocb **	allpcbs;
	struct iocb *	alliocbs;
};

// == =========================
long open_file_internal(char *path, int flags, int bufsize, int nbuffers, long estSize)
{
	int fd;
	int i;
	struct op_info * pinfo = NULL;

	bufsize = bufsize <= 0 ? 100 * 1024 : bufsize;

	if (path == NULL || nbuffers <= 0 || bufsize % BLOCK_SIZE != 0)
	{
		printf("Paramters error in open_file\n");
		return 0;
	}
	fd = open(path, flags, 0644);
	if (fd < 0)
	{
		return 0;
	}

	if (flags & O_CREAT)
	{
		ftruncate(fd, estSize);
	}

	pinfo = (struct op_info *)malloc(sizeof(struct op_info));
	memset(pinfo, 0, sizeof(struct op_info));
	if(io_setup(nbuffers, &pinfo->ctx) != 0)
	{
		printf("io_setup failed!\n");
		goto F_close;
	}
	pinfo->fd = fd;
	pinfo->bufsize = bufsize;
	pinfo->nbuffers = nbuffers;

	pinfo->buffers = (char **)malloc(sizeof(char*) * nbuffers);
	if (pinfo->buffers == NULL)
	{
		goto F_close;
	}
	memset(pinfo->buffers, 0, sizeof(char*) * nbuffers);

	for( i = 0; i < nbuffers; i++)
	{
		pinfo->buffers[i] = (char *)memalign(4096, bufsize);
		//printf("Buf --> %p  \n", pinfo->buffers[i]);
		if (pinfo->buffers[i] == NULL)
		{
			goto F_free_buffers;
		}
	}

	pinfo->events = (struct io_event *)malloc(sizeof(struct io_event) * nbuffers);
	if (pinfo->events == NULL)
	{
		goto F_free_buffers;
	}

	pinfo->allpcbs = (struct iocb **)malloc(sizeof(struct iocb *) * nbuffers);
	if (pinfo->allpcbs == NULL)
	{
		goto F_free_io_event;
	}

	pinfo->alliocbs = (struct iocb *)malloc(sizeof(struct iocb) * nbuffers);
	if (pinfo->alliocbs == NULL)
	{
		goto F_free_allpcbs;
	}
	memset(pinfo->alliocbs, 0, sizeof(struct iocb) * nbuffers);

	pinfo->freeindexs = (int *)malloc(sizeof(int) * nbuffers);
	if (pinfo->freeindexs == NULL)
	{
		goto F_free_alliocbs;
	}
	for (i = 0; i < nbuffers; i++)
	{
		pinfo->freeindexs[i] = i;
	}
	pinfo->bufwriting = pinfo->buffers[0];

	return (long)pinfo;

F_free_alliocbs:
	free(pinfo->alliocbs);

F_free_allpcbs:
	free(pinfo->allpcbs);

F_free_io_event:
	free(pinfo->events);

F_free_buffers:
	for( i = 0; i < nbuffers; i++)
	{
		if (pinfo->buffers[i] != NULL)
		{
			free(pinfo->buffers[i]);
		}
	}
	free(pinfo->buffers);
	pinfo->buffers = NULL;

F_close:
	close(fd);
	unlink(path);
	free(pinfo);
	return 0;
}

long create_file(char *path, int bufSize, int numConcurrent, long estSize)
{
	int flags = O_WRONLY | O_CREAT | O_DIRECT | O_TRUNC;
	return open_file_internal(path, flags, bufSize, numConcurrent, estSize);
}

long open_file(char *path, int bufSize, int numConcurrent)
{
	int flags = O_RDONLY | O_DIRECT;
	return open_file_internal(path, flags, bufSize, numConcurrent, 0);
}

int fire_write(long file, int onclose)
{
	int ret;
	struct iocb *freep;

	struct op_info *pinfo = (struct op_info*)(file);
	int useid;
	struct iocb * p;

	int writesize = pinfo->bufwritten;
	int nwait;
	int i, id;

	if (onclose)
	{
		writesize = ((writesize + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
	}

	if (writesize > 0)
	{
		useid = pinfo->freeindexs[pinfo->inflying];
		p = &(pinfo->alliocbs[useid]);
		p->aio_fildes = pinfo->fd;
		p->aio_lio_opcode = IOCB_CMD_PWRITE;
		p->aio_buf = (uint64_t)(pinfo->bufwriting);
		p->aio_offset = pinfo->offset;
		p->aio_nbytes = writesize;

		pinfo->allpcbs[0] = p;

		ret = io_submit(pinfo->ctx, 1, pinfo->allpcbs);
		if (ret != 1)
		{
			printf("io_submit error %d\n", ret);
			return -1;
		}
		pinfo->inflying++;
	}

	if (pinfo->nbuffers - pinfo->inflying <= 0 || (onclose && pinfo->inflying > 0))
	{
		nwait = onclose ? pinfo->inflying : 1;
		while (1)
		{
			ret = io_getevents(pinfo->ctx, nwait, pinfo->inflying, pinfo->events, NULL);
			if (ret == -EINTR)
			{
				continue;
			}
			break;
		}
		if (ret > 0)
		{
			for (i = 0; i < ret; i++)
			{
				freep = (struct iocb *)(pinfo->events[i].obj);
				id = freep - pinfo->alliocbs;
				pinfo->inflying--;
				pinfo->freeindexs[pinfo->inflying] = id;
			}
		}
		else if (ret < 0)
		{
			printf("io_getevents error %d \n", ret);
			return -1;
		}
	}

	if (writesize > 0)
	{
		useid = pinfo->freeindexs[pinfo->inflying];
		pinfo->bufwriting = pinfo->buffers[useid];
		pinfo->offset += pinfo->bufwritten;
		pinfo->bufwritten = 0;
	}

	if (onclose)
	{
		ftruncate(pinfo->fd, pinfo->offset);
	}
	return 1;
}

int get_file_fd(long file)
{
	struct op_info *pinfo = (struct op_info*)(file);
	return pinfo->fd;
}

long get_write_buffer(long file)
{
	struct op_info *pinfo = (struct op_info*)(file);
	return (long)(pinfo->bufwriting);
}

int write_buffered_data(long file, int written)
{
	int ret;
	struct op_info *pinfo = (struct op_info*)(file);
	pinfo->bufwritten = written;
	ret = fire_write(file, pinfo->bufsize != written);
	return ret > 0 ? written : ret;
}

int write_file(long file, void *pdata, int datalen)
{
	struct op_info *pinfo = (struct op_info*)(file);
	int dataleft = datalen;
	int bufleft = pinfo->bufsize - pinfo->bufwritten;
	char *pbuf = pinfo->bufwriting + pinfo->bufwritten;
	int tocopy;
	int written = 0;

	while (written < datalen)
	{
		tocopy = bufleft >  dataleft ? dataleft : bufleft;
		memcpy(pbuf, ((char*)pdata) + written, tocopy);

		dataleft -= tocopy;
		bufleft -= tocopy;
		pinfo->bufwritten += tocopy;

		if (bufleft > 0)
		{
			break;
		}
		else
		{
			if(fire_write(file, 0) < 0)
			{
				return written;
			}
			bufleft = pinfo->bufsize - pinfo->bufwritten;
			pbuf = pinfo->bufwriting + pinfo->bufwritten;
		}
		written += tocopy;
	}
	return datalen;
}

long read_file(long file, long offset, char *pbuf, long buflen)
{
	int i, ntosub, left, useid, ret, nwait, id;
	int npages;
	struct iocb * p;
	struct op_info *pinfo = (struct op_info*)(file);

	if (((long)pbuf) & (BLOCK_SIZE - 1) != 0)
	{
		return -1;
	}

	int halfwait = (pinfo->nbuffers + 1) / 2;
	struct iocb * freep;
	long totalleft, toread;


	npages = (buflen + BLOCK_SIZE - 1) / BLOCK_SIZE;
	totalleft = (long)(npages) * BLOCK_SIZE;

	while (totalleft > 0)
	{
		left = pinfo->nbuffers - pinfo->inflying;
		for (ntosub = 0; ntosub < left && totalleft > 0; ntosub++)
		{
			useid = pinfo->freeindexs[pinfo->inflying];
			p = &(pinfo->alliocbs[useid]);
			p->aio_fildes = pinfo->fd;
			p->aio_lio_opcode = IOCB_CMD_PREAD;
			p->aio_buf = (uint64_t)(pbuf);
			p->aio_offset = offset;

			toread = totalleft > pinfo->bufsize ? pinfo->bufsize : totalleft;
			p->aio_nbytes = toread;

			pinfo->allpcbs[ntosub] = p;

			pinfo->inflying++;
			offset += toread;
			pbuf += toread;
			totalleft -= toread;
		}

		if (ntosub > 0)
		{
			ret = io_submit(pinfo->ctx, ntosub, pinfo->allpcbs);
			if (ret != ntosub)
			{
				printf("io_submit error %d/%d\n", ret, ntosub);
				return -2;
			}
		}

		nwait = totalleft > 0 ? halfwait : pinfo->inflying;
		ret = io_getevents(pinfo->ctx, nwait, pinfo->inflying, pinfo->events, NULL);
		if ( ret > 0)
		{
			for (i = 0; i < ret; i++)
			{
				freep = (struct iocb *)(pinfo->events[i].obj);
				id = freep - pinfo->alliocbs;
				pinfo->inflying--;
				pinfo->freeindexs[pinfo->inflying] = id;
			}
		}
		else
		{
			printf("io_getevents error %d \n", ret);
			return -3;
		}
	}
	return buflen;
}

void close_file(long file)
{
	int i;
	struct op_info *pinfo = (struct op_info*)(file);
	if (pinfo == NULL)
	{
		return;
	}

	fire_write(file, 1);

	close(pinfo->fd);
	pinfo->fd = -1;
	io_destroy(pinfo->ctx);

	free(pinfo->freeindexs);
	free(pinfo->alliocbs);
	free(pinfo->allpcbs);
	free(pinfo->events);

	for( i = 0; i < pinfo->nbuffers; i++)
	{
		if (pinfo->buffers[i] != NULL)
		{
			free(pinfo->buffers[i]);
		}
	}
	free(pinfo->buffers);
	pinfo->buffers = NULL;

	free(pinfo);
}


JNIEXPORT jlong JNICALL Java_org_apache_hadoop_net_unix_DomainSocket_read_1sample_1data
  (JNIEnv * env, jclass obj, jlong file, jlong filelen, jlong bufaddr, jint num, jlong optional)
{
	return read_sampling_data(file, filelen, num, (char*)(long)bufaddr);
}

void quick_sort(long *pdata, int low, int high)
{
	int i = low, j = high;
	long key = pdata[low];
	if (low < high)
	{
		while (i < j)
		{
			while (i < j && pdata[j] >= key)
			{
				j--;
			}
			if (i < j)
			{
				pdata[i++] = pdata[j];
			}

			while (i < j && pdata[i] < key)
			{
				i++;
			}
			if (i < j)
			{
				pdata[j--] = pdata[i];
			}
		}
		pdata[i] = key;
		quick_sort(pdata, low, i - 1);
		quick_sort(pdata, i + 1, high);
	}
}

int read_sampling_data(long file, long filelen, int items, char* pbuffer)
{
	static int inited = 0;
	int i, j, ret;
	struct op_info * pinfo;
	struct iocb * freep;
	struct iocb * p;

	if (items == 0 || pbuffer == NULL)
	{
		return 0;
	}

	if (!inited)
	{
		srand(time(NULL));
		inited = 1;
	}

	pinfo = (struct op_info*)(file);
	long *poffsets = (long *)malloc(sizeof(long) * items);
	if (poffsets == NULL)
	{
		return -1; // oom
	}

	long off;
	int numgened = 0;
	while (numgened < items)
	{
		off = (((double)(rand())) / RAND_MAX) * (filelen - BLOCK_SIZE - 1000);
		off = off - (off % 100);

		if ((off & (~(long)(BLOCK_SIZE - 1))) != ((off + 99) & (~(long)(BLOCK_SIZE - 1))))
		{
			off += 100;
		}
		poffsets[numgened++] = off;
	}

	// sort
	quick_sort(poffsets, 0, items - 1);

	long alignaddr, tmpaddr1, tmpaddr2;

	int * pidx = (int *)malloc(sizeof(int) * (pinfo->nbuffers));
	if (pidx == NULL)
	{
		free(poffsets);
		return -2;
	}

	int toproc = 0;
	int left, ntosub, useid, nwait, id;
	int halfwait = (pinfo->nbuffers + 1) / 2;
	while (toproc < items)
	{
		left = pinfo->nbuffers - pinfo->inflying;
		for (ntosub = 0; ntosub < left && toproc < items; ntosub++)
		{
			useid = pinfo->freeindexs[pinfo->inflying];
			p = &(pinfo->alliocbs[useid]);
			p->aio_fildes = pinfo->fd;
			p->aio_lio_opcode = IOCB_CMD_PREAD;
			p->aio_buf = (uint64_t)(pinfo->buffers[useid]);

			alignaddr = poffsets[toproc] & (~(long)(BLOCK_SIZE - 1));
			pidx[useid] = toproc;
			toproc++;
			tmpaddr1 = alignaddr;
			while (toproc < items)
			{
				tmpaddr2 = poffsets[toproc] & (~(long)(BLOCK_SIZE - 1));
				if (tmpaddr2 - tmpaddr1 > BLOCK_SIZE || tmpaddr2 - alignaddr > pinfo->bufsize - BLOCK_SIZE)
				{
					break;
				}
				tmpaddr1 = tmpaddr2;
				toproc++;
			}
			p->aio_offset = alignaddr;
			p->aio_nbytes = tmpaddr1 - alignaddr + BLOCK_SIZE;

			pinfo->allpcbs[ntosub] = p;

			pinfo->inflying++;
		}

		if (ntosub > 0)
		{
			ret = io_submit(pinfo->ctx, ntosub, pinfo->allpcbs);
			if (ret != ntosub)
			{
				printf("io_submit error %d/%d\n", ret, ntosub);
				ret = -3;
				goto F_free;
			}
		}

		nwait = toproc < items ? halfwait : pinfo->inflying;
		ret = io_getevents(pinfo->ctx, nwait, pinfo->inflying, pinfo->events, NULL);
		if ( ret > 0)
		{
			for (i = 0; i < ret; i++)
			{
				freep = (struct iocb *)(pinfo->events[i].obj);
				id = freep - pinfo->alliocbs;

				for (j = pidx[id] ; j < items; j++)
				{
					tmpaddr1 = poffsets[j] - freep->aio_offset;
					if (tmpaddr1 >= freep->aio_nbytes)
					{
						break;
					}
					memcpy(pbuffer + j * 10, ((char *)(freep->aio_buf)) + tmpaddr1, 10);
				}

				pinfo->inflying--;
				pinfo->freeindexs[pinfo->inflying] = id;
			}
		}
		else
		{
			printf("io_getevents error %d \n", ret);
			ret = -4;
			goto F_free;
		}
	}
	ret = items;

F_free:
	free(pidx);
	free(poffsets);
	return ret;
}


JNIEXPORT jlong JNICALL Java_org_apache_hadoop_net_unix_DomainSocket_create_1file
  (JNIEnv * env, jclass obj, jstring filePath, jint bufSize, jint numConcurrent, jlong estSize, jlong mode)
{
	char *path;
	path = (*env)->GetStringUTFChars(env, filePath, NULL);
	long ret = create_file(path, bufSize, numConcurrent, estSize);
	(*env)->ReleaseStringUTFChars(env, filePath, path);
	return ret;
}

/*
 * Class:     org_apache_hadoop_net_unix_DomainSocket
 * Method:    write_file
 * Signature: (JJI)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_net_unix_DomainSocket_write_1file
  (JNIEnv * env, jclass obj, jlong file, jlong pdata, jint dataLen)
{
	return write_file(file, (void *)pdata, dataLen);
}

/*
 * Class:     org_apache_hadoop_net_unix_DomainSocket
 * Method:    close_file
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_net_unix_DomainSocket_close_1file
  (JNIEnv * env, jclass obj, jlong file)
{
	close_file(file);
}

/*
 * Class:     org_apache_hadoop_net_unix_DomainSocket
 * Method:    open_file
 * Signature: (Ljava/lang/String;II)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_net_unix_DomainSocket_open_1file
    (JNIEnv * env, jclass obj, jstring filePath, jint bufSize, jint numConcurrent)
{
	char *path;
	path = (*env)->GetStringUTFChars(env, filePath, NULL);
	long ret = open_file(path, bufSize, numConcurrent);
	(*env)->ReleaseStringUTFChars(env, filePath, path);
	return ret;
}

/*
 * Class:     org_apache_hadoop_net_unix_DomainSocket
 * Method:    read_file
 * Signature: (JJJJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_net_unix_DomainSocket_read_1file
  (JNIEnv * env, jclass obj, jlong file, jlong fileOffset, jlong bufAddr, jlong bufLen)
{
	return read_file(file, fileOffset, (char*)bufAddr, bufLen);
}

long write_sort_data(long file, int nItems, char *idxBase, char **idxChunks)
{
	int i;
	long index;
	long locationInfo, chunkIndex, chunkOff;
	char *pdata;

	char *pwritebuf;
	int writebuflen;
	int nused;
	int ncopy;

	for (i = 0; i < nItems; i++)
	{
		index = i * 2 + 1;
		locationInfo = *(long *)(idxBase + (index << 3));
		locationInfo &= 0xFFFFFFFFL;
		chunkIndex = locationInfo >> 23;
		chunkOff = locationInfo & 0x7FFFFFL;
		pdata = idxChunks[chunkIndex] + chunkOff * 100;

		if(write_file(file, pdata, 100) != 100)
		{
			return i;
		}
	}
	return i;
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_net_unix_DomainSocket_write_1sort_1data
  (JNIEnv * env, jclass obj, jlong file, jint nItems, jlong idxBase, jlong idxChunks, jlong optional)
{
	return write_sort_data(file, nItems, (char *)(long)idxBase, (char **)(long)idxChunks);
}

/*
 * Class:     org_apache_hadoop_net_unix_DomainSocket
 * Method:    resv_func
 * Signature: (JJJJJJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_net_unix_DomainSocket_resv_1func
  (JNIEnv * env, jclass obj, jlong arg1, jlong arg2, jlong arg3, jlong arg4, jlong arg5, jlong arg6)
{
	if (arg1 == 1)
	{
		return get_write_buffer(arg2);
	}
	else if (arg1 == 2)
	{
		return write_buffered_data(arg2, arg3);
	}
	return 0;
}

double getseconds(struct timeval *last, struct timeval *current) {
  int sec, usec;
  sec = current->tv_sec - last->tv_sec;
  usec = current->tv_usec - last->tv_usec;
  return ( (double)sec + usec*((double)(1e-6)) );
}

// -----------
inline unsigned long cycles() {
        unsigned int lo, hi;
        __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi) );
        return (((unsigned long)hi) << 32) | lo;
}

#define REC_SIZE 100
#define SORT_BUF_SIZE (REC_SIZE * BLOCK_SIZE)

#define F_IN_IO(X) (1 << (X))



#define REC_SIZE 100
#define SORT_BUF_SIZE (REC_SIZE * BLOCK_SIZE)

#define F_IN_IO(X) (1 << (X))

#define TIME_STAT 1

#ifdef TIME_STAT

#define FILL_DATA_BIO(X, Y, Z) \
{ \
	cycst = cycles(); \
	fill_data_BIO((X), (Y), (Z)); \
	cycles_read_total += cycles() - cycst; \
}

#define FILL_DATA(X, Y, Z) \
{ \
	cycst = cycles(); \
	fill_data((X), (Y), (Z)); \
	cycles_read_total += cycles() - cycst; \
}

#define FWRITE(A,B,C,D) \
{ \
	cycst = cycles(); \
	fwrite((A), (B), (C), (D)); \
	cycles_write_total += cycles() - cycst; \
}

#else
#define FILL_DATA_BIO(X, Y, Z) 	fill_data_BIO((X), (Y), (Z));
#define FILL_DATA(X, Y, Z) 		fill_data((X), (Y), (Z));
#define FWRITE(A,B,C,D) 		fwrite((A), (B), (C), (D))
#endif


struct merge_info {
	int 			n;
	int  			inflying;
	aio_context_t 	ctx;
	struct io_event * events;
	int *			fds;
	long * 			lens;
	long *			offs;
	int * 			states;
	unsigned char ** buffers;
	struct iocb * 	alliocbs;
	struct iocb ** 	allpcbs;
};

void release_merge_info(struct merge_info *p)
{
	io_destroy(p->ctx);
	free(p->events);
	free(p->fds);
	free(p->lens);
	free(p->offs);
	free(p->states);
	free(p->buffers);
	free(p->alliocbs);
	free(p->allpcbs);
}

int wait_for_comp(struct merge_info *pm, int idx, int bufidx)
{
	struct iocb * freep;
	int id, ib;
	int nwait = 1;
	int exit = 0;
	int i, ret, all = 0;

	if (idx < pm->n)
	{
		if (!(pm->states[idx] | F_IN_IO(bufidx)))
		{
			return 1;
		}
	}
	else
	{
		nwait = pm->inflying;
	}

	while (pm->inflying > 0 && !exit)
	{
		ret = io_getevents(pm->ctx, nwait, pm->inflying, pm->events, NULL);
		if ( ret > 0)
		{
			all += ret;
			for (i = 0; i < ret; i++)
			{
				freep = (struct iocb *)(pm->events[i].obj);
				id = freep - pm->alliocbs;
				ib = freep->aio_buf == (uint64_t)(pm->buffers[i]) ? 0 : 1;
				pm->states[id] &= ~(F_IN_IO(ib));
				exit += idx == id && bufidx == ib;
				pm->inflying--;
			}
		}
		else
		{
			return ret;
		}
	}
	return all;
}



int fill_data(struct merge_info *pm, int waitidx, int bufidx)
{
	int i, end, shdbufidx = bufidx, ret = 0;
	int toproc = 0;
	struct iocb * p;
	long tmp, left, toread;
	int sig = waitidx >=0 && waitidx < pm->n;
	//unsigned long cycst = cycles();

	if (sig)
	{
		i = waitidx;
		end = waitidx + 1;
		shdbufidx = bufidx ? 0 : 1;
	}
	else
	{
		i = 0;
		end = pm->n;
	}

	for(; i < end; i++)
	{
		left = pm->lens[i] - pm->offs[i];

		if (left > 0)
		{
			p = pm->alliocbs + i;
			p->aio_fildes = pm->fds[i];
			p->aio_lio_opcode = IOCB_CMD_PREAD;
			p->aio_buf = (uint64_t)(pm->buffers[i] + shdbufidx * SORT_BUF_SIZE);
			p->aio_offset = pm->offs[i];

			toread = left > SORT_BUF_SIZE ? SORT_BUF_SIZE : ((left + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
			p->aio_nbytes = toread;

			pm->offs[i] += toread;
			pm->states[i] |= F_IN_IO(shdbufidx);

			pm->allpcbs[toproc] = p;
			toproc++;
		}
	}

	if (toproc > 0)
	{
		ret = io_submit(pm->ctx, toproc, pm->allpcbs);
		if (ret != toproc)
		{
			ret = -3;
			return ret;
		}
		pm->inflying += ret;
	}

	if (waitidx >= 0)
	{
		ret = wait_for_comp(pm, waitidx, bufidx);
	}
	//cycles_read_total += cycles() - cycst;
	return ret;
}

int SMALLER(unsigned char *p1, unsigned char *p2)
{
	int i, ret = 0;
	return memcmp(p1, p2, 10) < 0 ? 1 : 0;

	for (i = 0;i < 10 && ret == 0; i++)
	{
		ret = ((int)p1[i]) - ((int)p2[i]);
	}
	return ret < 0 ? 1 : 0;
}

void fix_up_heap(unsigned char * buffer[], int currlen)
{
	int i = (currlen - 1) / 2;
	unsigned char * tmp = buffer[currlen];
	while (i >= 0 && SMALLER(tmp, buffer[i]))
	{
		buffer[currlen] = buffer[i];
		currlen = i;
		if (i > 0)
		{
			i = (i - 1) / 2;
		}
		else
		{
			break;
		}
	}
	buffer[currlen] = tmp;
}

void fix_down_heap(unsigned char * buffer[], int currlen, int idx)
{
	int i = 2 * idx + 1;
	unsigned char * tmp = buffer[idx];
	while (i < currlen)
	{
		if (i < currlen - 1 && SMALLER(buffer[i + 1], buffer[i]))
			++i;
		if (SMALLER(buffer[i], tmp))
		{
			buffer[idx] = buffer[i];
			idx = i;
			i = 2 * idx + 1;
		}
		else
		{
			break;
		}
	}
	buffer[idx] = tmp;
}

void insert_heap(unsigned char * buffer[], int bufferSize, int * currlen, unsigned char * value)
{
	buffer[(*currlen)++] = value;
	fix_up_heap(buffer, *currlen - 1);
}

void pop_heap(unsigned char * buffer[], int * currlen)
{
	buffer[0] = buffer[--*currlen];
	fix_down_heap(buffer, *currlen, 0);
}


void fix_up_heap_with_idx(unsigned char * buffer[], int idxbuf[], int currlen)
{
	int i = (currlen - 1) / 2;
	unsigned char * tmp = buffer[currlen];
	int tmpidx = idxbuf[currlen];
	while (i >= 0 && SMALLER(tmp, buffer[i]))
	{
		buffer[currlen] = buffer[i];
		idxbuf[currlen] = idxbuf[i];
		currlen = i;
		if (i > 0)
		{
			i = (i - 1) / 2;
		}
		else
		{
			break;
		}
	}
	buffer[currlen] = tmp;
	idxbuf[currlen] = tmpidx;
}

void fix_down_heap_with_idx(unsigned char * buffer[], int idxbuf[], int currlen, int idx)
{
	int i = 2 * idx + 1;
	unsigned char * tmp = buffer[idx];
	int tmpidx = idxbuf[idx];
	while (i < currlen)
	{
		if (i < currlen - 1 && SMALLER(buffer[i + 1], buffer[i]))
		{
			++i;
		}

		if (SMALLER(buffer[i], tmp))
		{
			buffer[idx] = buffer[i];
			idxbuf[idx] = idxbuf[i];
			idx = i;
			i = 2 * idx + 1;
		}
		else
		{
			break;
		}
	}
	buffer[idx] = tmp;
	idxbuf[idx] = tmpidx;
}

void insert_heap_with_idx(unsigned char * buffer[], int idxbuf[], int bufferSize, int * currlen, unsigned char * value, int idx)
{
	buffer[*currlen] = value;
	idxbuf[*currlen] = idx;
	(*currlen)++;
	fix_up_heap_with_idx(buffer, idxbuf, *currlen - 1);
}

void pop_heap_with_idx(unsigned char * buffer[], int idxbuf[], int * currlen)
{
	--(*currlen);
	buffer[0] = buffer[*currlen];
	idxbuf[0] = idxbuf[*currlen];
	fix_down_heap_with_idx(buffer, idxbuf, *currlen, 0);
}


int merge_sort_files(char *pathout, char *pathin[], int numInputs, unsigned char *pext, int records)
{

	int numinfiles = 0;
	int ret = records, tmp, i, j;
	char *ptmp;

#ifdef TIME_STAT
	struct timeval tvs, tve;
	gettimeofday(&tvs, NULL);
	unsigned long cycst, cycles_total = cycles();
	unsigned long cycles_read_total = 0, cycles_write_total = 0;
	char logbuf[1024];
#endif

	FILE *fpout;

	if ((fpout=fopen(pathout, "wb")) == NULL)
	{
		return -1;
	}

	struct merge_info pminfo;
	struct merge_info *pm = &pminfo;
	memset(pm, 0, sizeof(struct merge_info));

	if(io_setup(numInputs, &pm->ctx) != 0)
	{
		printf("io_setup failed!\n");
		return 0;
	}

	pm->n = numInputs;
	pm->fds = (int *)malloc(sizeof(int) * numInputs);
	pm->offs = (long *)malloc(sizeof(long) * numInputs);
	memset(pm->offs, 0, sizeof(long) * numInputs);
	pm->lens = (long *)malloc(sizeof(long) * numInputs);
	pm->buffers = (unsigned char **)malloc(sizeof(unsigned char *) * numInputs);
	pm->states = (int *)malloc(sizeof(int) * numInputs);
	memset(pm->states, 0, sizeof(int) * numInputs);
	pm->alliocbs = (struct iocb *)malloc(sizeof(struct iocb) * numInputs);
	memset(pm->alliocbs, 0 , sizeof(struct iocb) * numInputs);
	pm->allpcbs = (struct iocb **)malloc(sizeof(struct iocb *) * numInputs);
	pm->events = (struct io_event *)malloc(sizeof(struct io_event) * numInputs);

	int *recs = (int *)malloc(sizeof(int) * (numInputs));
	unsigned char *dbuf = (unsigned char *)memalign(4096, SORT_BUF_SIZE * numInputs * 2);
	int *pbufoffs = (int *)malloc(sizeof(int) * (numInputs));
	memset(pbufoffs, 0, sizeof(int) * (numInputs));
	unsigned char ** pbufbases = (unsigned char **)malloc(sizeof(unsigned char *) * (numInputs));
	//memset(pbufbases, 0, sizeof(unsigned char *) * (numInputs));

	// if (!(pm->fds && pm->offs && pm->lens && pm->buffers &&
	    // pm->states && pm->alliocbs && pm->allpcbs && pm->events &&
		// recs && dbuf && pbufoffs && pbufbases))
	// {
		// sprintf(logbuf, "Memory alloc error: dbuf=%p \n", dbuf);
		// fwrite(logbuf, strlen(logbuf), 1, fpout); fflush(fpout);
	// }

	for (i = 0; i < numInputs; i++)
	{
		pbufbases[i] = dbuf + i * SORT_BUF_SIZE * 2;
		pm->buffers[i] = pbufbases[i];
	}

	for (i = 0; i < numInputs; i++)
	{
		pm->fds[numinfiles] = open(pathin[i], O_RDONLY | O_DIRECT);
		if (pm->fds[numinfiles] < 0)
		{
			ret = -1000 - (numinfiles + 1);
			goto F_err_open;
		}
		pm->lens[numinfiles] = lseek(pm->fds[numinfiles], 0, SEEK_END);
		recs[numinfiles] = pm->lens[numinfiles] / REC_SIZE;
		ret += recs[numinfiles];
		numinfiles++;
	}

	FILL_DATA(pm, pm->n + 10, 0);
	FILL_DATA(pm, -1, 1);

	int capSize = (numInputs + 2);
	int inheap = 0;
	int id;
	unsigned char **pkeys = (unsigned char **)malloc(sizeof(unsigned char*) * capSize);

	int _items = numInputs; // + (records ? 1 : 0)
	for (i = 0; i < _items; i++)
	{
		if (recs[i] > 0)
		{
			insert_heap(pkeys, capSize, &inheap, pbufbases[i]);
			recs[i]--;
		}
	}

	while (1)
	{
		if (records > 0)
		{
			if (inheap > 0 && !SMALLER(pkeys[0], pext))
			{
				FWRITE(pext, REC_SIZE, 1, fpout);
				records--;
				pext += REC_SIZE;
				continue;
			}
		}

		if (inheap > 0)
		{
			id = (pkeys[0] - dbuf) / (SORT_BUF_SIZE * 2);
			FWRITE(pkeys[0], REC_SIZE, 1, fpout);
			pop_heap(pkeys, &inheap);

			if (recs[id] > 0)
			{
				pbufoffs[id] += REC_SIZE;
				if (pbufoffs[id] == SORT_BUF_SIZE)
				{
					FILL_DATA(pm, id, 1);
				}
				else if (pbufoffs[id] == SORT_BUF_SIZE * 2)
				{
					FILL_DATA(pm, id, 0);
					pbufoffs[id] = 0;
				}
				insert_heap(pkeys, capSize, &inheap, pbufbases[id] + pbufoffs[id]);
				recs[id]--;
			}
		}
		else
		{
			break;
		}
	}


	if (records > 0)
	{
		FWRITE(pext, REC_SIZE * records, 1, fpout);
	}

	free(pkeys);
	free(recs);
	free(dbuf);
	free(pbufoffs);
	free(pbufbases);

#ifdef TIME_STAT
	cycles_total = cycles() - cycles_total;
	gettimeofday(&tve, NULL);
	sprintf(logbuf, "\n%d %llu %llu %llu %0.2f %0.2f\n",
					ret, getseconds(&tvs, &tve), cycles_total, cycles_read_total, cycles_write_total,
					cycles_read_total * 1.0 / cycles_total * 100,
					cycles_write_total * 1.0 / cycles_total * 100);
	fwrite(logbuf, strlen(logbuf), 1, fpout);
#endif

F_err_open:;
	for( i = 0; i < numinfiles; i++)
	{
		close(pm->fds[i]);
	}
	release_merge_info(pm);
	fclose(fpout);
	return ret;
}

#define BUF_SIZE_BYTES (100 * 4096)

int fill_data_BIO(struct merge_info *pm, int waitidx, int bufidx)
{
	int i, end, shdbufidx = bufidx, ret = 0;
	int toproc = 0;
	struct iocb * p;
	long tmp, left, toread;
	int sig = waitidx >=0 && waitidx < pm->n;
	//unsigned long cycst = cycles();

	if (sig)
	{
		i = waitidx;
		end = waitidx + 1;
	}
	else
	{
		i = 0;
		end = pm->n;
	}

	for(; i < end; i++)
	{
		read(pm->fds[i], pm->buffers[i], BUF_SIZE_BYTES);
	}
	//cycles_read_total += cycles() - cycst;
	return 0;
}

int merge_sort_files_BIO(char *pathout, char *pathin[], int numInputs, unsigned char *pext, int records)
{
	int numinfiles = 0;
	int ret = records, tmp, i, j;
	char *ptmp;

#ifdef TIME_STAT
	struct timeval tvs, tve;
	gettimeofday(&tvs, NULL);
	unsigned long cycst, cycles_total = cycles();
	unsigned long cycles_read_total = 0, cycles_write_total = 0;
	char logbuf[1024];
#endif

	FILE *fpout;

	if ((fpout=fopen(pathout, "wb")) == NULL)
	{
		return -1;
	}

	struct merge_info pminfo;
	struct merge_info *pm = &pminfo;
	memset(pm, 0, sizeof(struct merge_info));

	pm->n = numInputs;
	pm->fds = (int *)malloc(sizeof(int) * numInputs);
	pm->buffers = (unsigned char **)malloc(sizeof(unsigned char *) * numInputs);

	int *recs = (int *)malloc(sizeof(int) * (numInputs));
	//unsigned char *dbuf = (unsigned char *)memalign(4096, SORT_BUF_SIZE * numInputs * 2);
	unsigned char *dbuf = (unsigned char*)memalign(4096, BUF_SIZE_BYTES * numInputs);
	int *pbufoffs = (int *)malloc(sizeof(int) * (numInputs));
	memset(pbufoffs, 0, sizeof(int) * (numInputs));
	unsigned char ** pbufbases = (unsigned char **)malloc(sizeof(unsigned char *) * (numInputs));
	//memset(pbufbases, 0, sizeof(unsigned char *) * (numInputs));

	for (i = 0; i < numInputs; i++)
	{
		pbufbases[i] = dbuf + i * BUF_SIZE_BYTES; // i * SORT_BUF_SIZE * 2;
		pm->buffers[i] = pbufbases[i];
	}

	for (i = 0; i < numInputs; i++)
	{
		pm->fds[numinfiles] = open(pathin[i], O_RDONLY); // | O_DIRECT);
		if (pm->fds[numinfiles] < 0)
		{
			ret = -1000 - (numinfiles + 1);
			goto F_err_open;
		}
		recs[numinfiles] = lseek(pm->fds[numinfiles], 0, SEEK_END) / REC_SIZE;
		ret += recs[numinfiles];
		lseek(pm->fds[numinfiles], 0, SEEK_SET);
		numinfiles++;
	}

	FILL_DATA_BIO(pm, pm->n + 10, 0);


	int capSize = (numInputs + 2);
	int inheap = 0;
	int id;
	unsigned char **pkeys = (unsigned char **)malloc(sizeof(unsigned char*) * capSize);

	int _items = numInputs; // + (records ? 1 : 0)
	for (i = 0; i < _items; i++)
	{
		if (recs[i] > 0)
		{
			insert_heap(pkeys, capSize, &inheap, pbufbases[i]);
			recs[i]--;
		}
	}

	while (1)
	{
		if (records > 0)
		{
			if (inheap > 0 && !SMALLER(pkeys[0], pext))
			{
				FWRITE(pext, REC_SIZE, 1, fpout);
				records--;
				pext += REC_SIZE;
				continue;
			}
		}

		if (inheap > 0)
		{
			id = (pkeys[0] - dbuf) / BUF_SIZE_BYTES;
			FWRITE(pkeys[0], REC_SIZE, 1, fpout);
			pop_heap(pkeys, &inheap);

			if (recs[id] > 0)
			{
				pbufoffs[id] += REC_SIZE;
				if (pbufoffs[id] == BUF_SIZE_BYTES)
				{
					FILL_DATA_BIO(pm, id, 0);
					pbufoffs[id] = 0;
				}
				insert_heap(pkeys, capSize, &inheap, pbufbases[id] + pbufoffs[id]);
				recs[id]--;
			}
		}
		else
		{
			break;
		}
	}


	if (records > 0)
	{
		FWRITE(pext, REC_SIZE * records, 1, fpout);
	}

#ifdef TIME_STAT
	cycles_total = cycles() - cycles_total;
	gettimeofday(&tve, NULL);
	sprintf(logbuf, "\n%d %f %llu %llu %llu %0.2f %0.2f\n",
					ret, getseconds(&tvs, &tve), cycles_total, cycles_read_total, cycles_write_total,
					cycles_read_total * 1.0 / cycles_total * 100,
					cycles_write_total * 1.0 / cycles_total * 100);
	fwrite(logbuf, strlen(logbuf), 1, fpout);
#endif

	free(pkeys);
	free(recs);
	free(dbuf);
	free(pbufoffs);
	free(pbufbases);

F_err_open:;
	for( i = 0; i < numinfiles; i++)
	{
		close(pm->fds[i]);
	}
	//release_merge_info(pm);
	free(pm->buffers);
	free(pm->fds);
	fclose(fpout);
	return ret;
}

int merge_sort_mem(char *pathout, unsigned char **pb, int *recs, int nitems)
{
	int ret, i;
	FILE *fpout;

#ifdef TIME_STAT
	struct timeval tvs, tve;
	gettimeofday(&tvs, NULL);
	unsigned long cycst, cycles_total = cycles();
	unsigned long cycles_read_total = 0, cycles_write_total = 0;
	char logbuf[1024];
#endif

	if ((fpout=fopen(pathout, "wb")) == NULL)
	{
		return -100001;
	}

	int inheap = 0;
	int capSize = (nitems + 2);
	unsigned char *ptmp;
	int id;
	unsigned char **heap = (unsigned char **)malloc(sizeof(unsigned char *) * capSize);
	int *idxbuf = (int *)malloc(sizeof(int) * capSize);
	if (heap == NULL)
	{
		ret = -100002;
		goto F_close;
	}

	for (i = 0; i < nitems; i++)
	{
		if (recs[i] > 0)
		{
			insert_heap_with_idx(heap, idxbuf, capSize, &inheap, pb[i], i);
			recs[i]--;
		}
	}

	ret = 0;
	while(inheap > 1)
	{
		ret++;
		ptmp = heap[0];
		id = idxbuf[0];
		FWRITE(ptmp, REC_SIZE, 1, fpout);
		pop_heap_with_idx(heap, idxbuf, &inheap);

		if(recs[id] > 0)
		{
			insert_heap_with_idx(heap, idxbuf, capSize, &inheap, ptmp + REC_SIZE, id);
			recs[id]--;
		}
	}

	if (inheap == 1)
	{
		FWRITE(heap[0], REC_SIZE * (recs[idxbuf[0]] + 1), 1, fpout);
		ret += recs[idxbuf[0]] + 1;
	}

	free(idxbuf);
	free(heap);

#ifdef TIME_STAT
	cycles_total = cycles() - cycles_total;
	gettimeofday(&tve, NULL);

	*((unsigned int *)(logbuf + 0)) = 0xFFFFFFFF;
	*((unsigned int *)(logbuf + 4)) = 0xFFFFFFFF;
	*((unsigned int *)(logbuf + 8)) = 0xFFFFFFFF;
	sprintf(&logbuf[12], "\n%d %f %llu %llu %llu %0.2f %0.2f\n",
					ret, getseconds(&tvs, &tve), cycles_total, cycles_read_total, cycles_write_total,
					cycles_read_total * 1.0 / cycles_total * 100,
					cycles_write_total * 1.0 / cycles_total * 100);
	//fwrite(logbuf, (strlen(logbuf) + (REC_SIZE - 1)) / REC_SIZE * REC_SIZE, 1, fpout);
	fwrite(logbuf, REC_SIZE, 1, fpout);
#endif

F_close:
	fclose(fpout);
	return ret;
}



JNIEXPORT jlong JNICALL Java_org_apache_hadoop_net_unix_DomainSocket_mege_1files
  (JNIEnv * env, jclass obj, jstring outpath, jobjectArray inpaths, jlong addr, jint items, jint bufsize)
{
	int i;
	long ret = 0;
	char *pout;
	int numin = (*env)->GetArrayLength(env, inpaths);
	jstring *pinjs = (jstring *)malloc(sizeof(jstring)* numin);
	char **pins = (char**)malloc(sizeof(char *) * numin);

	for (i=0; i<numin; i++) {
		pinjs[i] = (jstring) ((*env)->GetObjectArrayElement(env, inpaths, i));
		pins[i] = (*env)->GetStringUTFChars(env, pinjs[i], 0);
		// Don't forget to call `ReleaseStringUTFChars` when you're done.
    }
	pout = (*env)->GetStringUTFChars(env, outpath, NULL);

	ret =  merge_sort_files_BIO(pout, pins, numin, (unsigned char*)addr, items);

	(*env)->ReleaseStringUTFChars(env, outpath, pout);
	for (i = 0; i < numin; i++) {
		(*env)->ReleaseStringUTFChars(env, pinjs[i], pins[i]);
    }
	free(pinjs);
	free(pins);
	return ret;
}


JNIEXPORT jlong JNICALL Java_org_apache_hadoop_net_unix_DomainSocket_merge_1sort
  (JNIEnv *env, jclass obj, jstring outpath, jlongArray jaddrs, jlongArray jrecords, jlong resv)
{
	long ret = 0;
	char *pout = (*env)->GetStringUTFChars(env, outpath, NULL);
	jsize nblocks = (*env)->GetArrayLength(env, jaddrs);
	jlong *blocks = (*env)->GetLongArrayElements(env, jaddrs, 0);
	jsize narryrecs = (*env)->GetArrayLength(env, jrecords);
	jint *nrecs = (*env)->GetIntArrayElements(env, jrecords, 0);

	if(nblocks == narryrecs)
	{
		ret = merge_sort_mem(pout, (unsigned char **)blocks, (int *)nrecs, nblocks);
	}
	else
	{
		ret = -1010;
	}

	(*env)->ReleaseIntArrayElements(env, jrecords, nrecs, 0);
	(*env)->ReleaseLongArrayElements(env, jaddrs, blocks, 0);
	(*env)->ReleaseStringUTFChars(env, outpath, pout);
	return ret;
}

//==========================================================

#define SOCKETPAIR_ARRAY_LEN 2

JNIEXPORT jarray JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_socketpair0(
JNIEnv *env, jclass clazz)
{
  jarray arr = NULL;
  int idx, err, fds[SOCKETPAIR_ARRAY_LEN] = { -1, -1 };
  jthrowable jthr = NULL;

  arr = (*env)->NewIntArray(env, SOCKETPAIR_ARRAY_LEN);
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->ExceptionClear(env);
    goto done;
  }
  if (socketpair(PF_UNIX, SOCK_STREAM, 0, fds) < 0) {
    err = errno;
    jthr = newSocketException(env, err,
            "socketpair(2) error: %s", terror(err));
    goto done;
  }
  (*env)->SetIntArrayRegion(env, arr, 0, SOCKETPAIR_ARRAY_LEN, fds);
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->ExceptionClear(env);
    goto done;
  }

done:
  if (jthr) {
    (*env)->DeleteLocalRef(env, arr);
    arr = NULL;
    for (idx = 0; idx < SOCKETPAIR_ARRAY_LEN; idx++) {
      if (fds[idx] >= 0) {
        close(fds[idx]);
        fds[idx] = -1;
      }
    }
    (*env)->Throw(env, jthr);
  }
  return arr;
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_accept0(
JNIEnv *env, jclass clazz, jint fd)
{
  int ret, newFd = -1;
  socklen_t slen;
  struct sockaddr_un remote;
  jthrowable jthr = NULL;

  slen = sizeof(remote);
  do {
    newFd = accept(fd, (struct sockaddr*)&remote, &slen);
  } while ((newFd < 0) && (errno == EINTR));
  if (newFd < 0) {
    ret = errno;
    jthr = newSocketException(env, ret, "accept(2) error: %s", terror(ret));
    goto done;
  }

done:
  if (jthr) {
    if (newFd > 0) {
      RETRY_ON_EINTR(ret, close(newFd));
      newFd = -1;
    }
    (*env)->Throw(env, jthr);
  }
  return newFd;
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_connect0(
JNIEnv *env, jclass clazz, jstring path)
{
  int ret, fd;
  jthrowable jthr = NULL;

  jthr = setup(env, &fd, path, 1);
  if (jthr) {
    (*env)->Throw(env, jthr);
    return -1;
  }
  if (((jthr = setAttribute0(env, fd, SEND_TIMEOUT, DEFAULT_SEND_TIMEOUT))) ||
      ((jthr = setAttribute0(env, fd, RECEIVE_TIMEOUT, DEFAULT_RECEIVE_TIMEOUT)))) {
    RETRY_ON_EINTR(ret, close(fd));
    (*env)->Throw(env, jthr);
    return -1;
  }
  return fd;
}

static void javaMillisToTimeVal(int javaMillis, struct timeval *tv)
{
  tv->tv_sec = javaMillis / 1000;
  tv->tv_usec = (javaMillis - (tv->tv_sec * 1000)) * 1000;
}

static jthrowable setAttribute0(JNIEnv *env, jint fd, jint type, jint val)
{
  struct timeval tv;
  int ret, buf;

  switch (type) {
  case SEND_BUFFER_SIZE:
    buf = val;
    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf))) {
      ret = errno;
      return newSocketException(env, ret,
          "setsockopt(SO_SNDBUF) error: %s", terror(ret));
    }
    return NULL;
  case RECEIVE_BUFFER_SIZE:
    buf = val;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf))) {
      ret = errno;
      return newSocketException(env, ret,
          "setsockopt(SO_RCVBUF) error: %s", terror(ret));
    }
    return NULL;
  case SEND_TIMEOUT:
    javaMillisToTimeVal(val, &tv);
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (struct timeval *)&tv,
               sizeof(tv))) {
      ret = errno;
      return newSocketException(env, ret,
          "setsockopt(SO_SNDTIMEO) error: %s", terror(ret));
    }
    return NULL;
  case RECEIVE_TIMEOUT:
    javaMillisToTimeVal(val, &tv);
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (struct timeval *)&tv,
               sizeof(tv))) {
      ret = errno;
      return newSocketException(env, ret,
          "setsockopt(SO_RCVTIMEO) error: %s", terror(ret));
    }
    return NULL;
  default:
    break;
  }
  return newRuntimeException(env, "Invalid attribute type %d.", type);
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_setAttribute0(
JNIEnv *env, jclass clazz, jint fd, jint type, jint val)
{
  jthrowable jthr = setAttribute0(env, fd, type, val);
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
}

static jint getSockOptBufSizeToJavaBufSize(int size)
{
#ifdef __linux__
  // Linux always doubles the value that you set with setsockopt.
  // We cut it in half here so that programs can at least read back the same
  // value they set.
  size /= 2;
#endif
  return size;
}

static int timeValToJavaMillis(const struct timeval *tv)
{
  return (tv->tv_sec * 1000) + (tv->tv_usec / 1000);
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_getAttribute0(
JNIEnv *env, jclass clazz, jint fd, jint type)
{
  struct timeval tv;
  socklen_t len;
  int ret, rval = 0;

  switch (type) {
  case SEND_BUFFER_SIZE:
    len = sizeof(rval);
    if (getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &rval, &len)) {
      ret = errno;
      (*env)->Throw(env, newSocketException(env, ret,
          "getsockopt(SO_SNDBUF) error: %s", terror(ret)));
      return -1;
    }
    return getSockOptBufSizeToJavaBufSize(rval);
  case RECEIVE_BUFFER_SIZE:
    len = sizeof(rval);
    if (getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rval, &len)) {
      ret = errno;
      (*env)->Throw(env, newSocketException(env, ret,
          "getsockopt(SO_RCVBUF) error: %s", terror(ret)));
      return -1;
    }
    return getSockOptBufSizeToJavaBufSize(rval);
  case SEND_TIMEOUT:
    memset(&tv, 0, sizeof(tv));
    len = sizeof(struct timeval);
    if (getsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, &len)) {
      ret = errno;
      (*env)->Throw(env, newSocketException(env, ret,
          "getsockopt(SO_SNDTIMEO) error: %s", terror(ret)));
      return -1;
    }
    return timeValToJavaMillis(&tv);
  case RECEIVE_TIMEOUT:
    memset(&tv, 0, sizeof(tv));
    len = sizeof(struct timeval);
    if (getsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, &len)) {
      ret = errno;
      (*env)->Throw(env, newSocketException(env, ret,
          "getsockopt(SO_RCVTIMEO) error: %s", terror(ret)));
      return -1;
    }
    return timeValToJavaMillis(&tv);
  default:
    (*env)->Throw(env, newRuntimeException(env,
          "Invalid attribute type %d.", type));
    return -1;
  }
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_close0(
JNIEnv *env, jclass clazz, jint fd)
{
  int ret;

  RETRY_ON_EINTR(ret, close(fd));
  if (ret) {
    ret = errno;
    (*env)->Throw(env, newSocketException(env, ret,
          "close(2) error: %s", terror(ret)));
  }
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_closeFileDescriptor0(
JNIEnv *env, jclass clazz, jobject jfd)
{
  Java_org_apache_hadoop_net_unix_DomainSocket_close0(
      env, clazz, fd_get(env, jfd));
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_shutdown0(
JNIEnv *env, jclass clazz, jint fd)
{
  int ret;

  RETRY_ON_EINTR(ret, shutdown(fd, SHUT_RDWR));
  if (ret) {
    ret = errno;
    (*env)->Throw(env, newSocketException(env, ret,
          "shutdown(2) error: %s", terror(ret)));
  }
}

/**
 * Write an entire buffer to a file descriptor.
 *
 * @param env            The JNI environment.
 * @param fd             The fd to write to.
 * @param buf            The buffer to write
 * @param amt            The length of the buffer to write.
 * @return               NULL on success; or the unraised exception representing
 *                       the problem.
 */
static jthrowable write_fully(JNIEnv *env, int fd, int8_t *buf, int amt)
{
  int err, res;

  while (amt > 0) {
    res = send(fd, buf, amt, PLATFORM_SEND_FLAGS);
    if (res < 0) {
      err = errno;
      if (err == EINTR) {
        continue;
      }
      return newSocketException(env, err, "write(2) error: %s", terror(err));
    }
    amt -= res;
    buf += res;
  }
  return NULL;
}

/**
 * Our auxillary data setup.
 *
 * See man 3 cmsg for more information about auxillary socket data on UNIX.
 *
 * We use __attribute__((packed)) to ensure that the compiler doesn't insert any
 * padding between 'hdr' and 'fds'.
 * We use __attribute__((aligned(8)) to ensure that the compiler puts the start
 * of the structure at an address which is a multiple of 8.  If we did not do
 * this, the attribute((packed)) would cause the compiler to generate a lot of
 * slow code for accessing unaligned memory.
 */
struct cmsghdr_with_fds {
  struct cmsghdr hdr;
  int fds[MAX_PASSED_FDS];
}  __attribute__((packed,aligned(8)));

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_sendFileDescriptors0(
JNIEnv *env, jclass clazz, jint fd, jobject jfds, jobject jbuf,
jint offset, jint length)
{
  struct iovec vec[1];
  struct flexibleBuffer flexBuf;
  struct cmsghdr_with_fds aux;
  jint jfdsLen;
  int i, ret = -1, auxLen;
  struct msghdr socketMsg;
  jthrowable jthr = NULL;

  jthr = flexBufInit(env, &flexBuf, length);
  if (jthr) {
    goto done;
  }
  if (length <= 0) {
    jthr = newException(env, "java/lang/IllegalArgumentException",
        "You must write at least one byte.");
    goto done;
  }
  jfdsLen = (*env)->GetArrayLength(env, jfds);
  if (jfdsLen <= 0) {
    jthr = newException(env, "java/lang/IllegalArgumentException",
        "Called sendFileDescriptors with no file descriptors.");
    goto done;
  } else if (jfdsLen > MAX_PASSED_FDS) {
    jfdsLen = 0;
    jthr = newException(env, "java/lang/IllegalArgumentException",
          "Called sendFileDescriptors with an array of %d length.  "
          "The maximum is %d.", jfdsLen, MAX_PASSED_FDS);
    goto done;
  }
  (*env)->GetByteArrayRegion(env, jbuf, offset, length, flexBuf.curBuf);
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->ExceptionClear(env);
    goto done;
  }
  memset(&vec, 0, sizeof(vec));
  vec[0].iov_base = flexBuf.curBuf;
  vec[0].iov_len = length;
  auxLen = CMSG_LEN(jfdsLen * sizeof(int));
  memset(&aux, 0, auxLen);
  memset(&socketMsg, 0, sizeof(socketMsg));
  socketMsg.msg_iov = vec;
  socketMsg.msg_iovlen = 1;
  socketMsg.msg_control = &aux;
  socketMsg.msg_controllen = auxLen;
  aux.hdr.cmsg_len = auxLen;
  aux.hdr.cmsg_level = SOL_SOCKET;
  aux.hdr.cmsg_type = SCM_RIGHTS;
  for (i = 0; i < jfdsLen; i++) {
    jobject jfd = (*env)->GetObjectArrayElement(env, jfds, i);
    if (!jfd) {
      jthr = (*env)->ExceptionOccurred(env);
      if (jthr) {
        (*env)->ExceptionClear(env);
        goto done;
      }
      jthr = newException(env, "java/lang/NullPointerException",
            "element %d of jfds was NULL.", i);
      goto done;
    }
    aux.fds[i] = fd_get(env, jfd);
    (*env)->DeleteLocalRef(env, jfd);
    if (jthr) {
      goto done;
    }
  }
  RETRY_ON_EINTR(ret, sendmsg(fd, &socketMsg, PLATFORM_SEND_FLAGS));
  if (ret < 0) {
    ret = errno;
    jthr = newSocketException(env, ret, "sendmsg(2) error: %s", terror(ret));
    goto done;
  }
  length -= ret;
  if (length > 0) {
    // Write the rest of the bytes we were asked to send.
    // This time, no fds will be attached.
    jthr = write_fully(env, fd, flexBuf.curBuf + ret, length);
    if (jthr) {
      goto done;
    }
  }

done:
  flexBufFree(&flexBuf);
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_receiveFileDescriptors0(
JNIEnv *env, jclass clazz, jint fd, jarray jfds, jarray jbuf,
jint offset, jint length)
{
  struct iovec vec[1];
  struct flexibleBuffer flexBuf;
  struct cmsghdr_with_fds aux;
  int i, jRecvFdsLen = 0, auxLen;
  jint jfdsLen = 0;
  struct msghdr socketMsg;
  ssize_t bytesRead = -1;
  jobject fdObj;
  jthrowable jthr = NULL;

  jthr = flexBufInit(env, &flexBuf, length);
  if (jthr) {
    goto done;
  }
  if (length <= 0) {
    jthr = newRuntimeException(env, "You must read at least one byte.");
    goto done;
  }
  jfdsLen = (*env)->GetArrayLength(env, jfds);
  if (jfdsLen <= 0) {
    jthr = newException(env, "java/lang/IllegalArgumentException",
        "Called receiveFileDescriptors with an array of %d length.  "
        "You must pass at least one fd.", jfdsLen);
    goto done;
  } else if (jfdsLen > MAX_PASSED_FDS) {
    jfdsLen = 0;
    jthr = newException(env, "java/lang/IllegalArgumentException",
          "Called receiveFileDescriptors with an array of %d length.  "
          "The maximum is %d.", jfdsLen, MAX_PASSED_FDS);
    goto done;
  }
  for (i = 0; i < jfdsLen; i++) {
    (*env)->SetObjectArrayElement(env, jfds, i, NULL);
  }
  vec[0].iov_base = flexBuf.curBuf;
  vec[0].iov_len = length;
  auxLen = CMSG_LEN(jfdsLen * sizeof(int));
  memset(&aux, 0, auxLen);
  memset(&socketMsg, 0, auxLen);
  socketMsg.msg_iov = vec;
  socketMsg.msg_iovlen = 1;
  socketMsg.msg_control = &aux;
  socketMsg.msg_controllen = auxLen;
  aux.hdr.cmsg_len = auxLen;
  aux.hdr.cmsg_level = SOL_SOCKET;
  aux.hdr.cmsg_type = SCM_RIGHTS;
  RETRY_ON_EINTR(bytesRead, recvmsg(fd, &socketMsg, 0));
  if (bytesRead < 0) {
    int ret = errno;
    if (ret == ECONNABORTED) {
      // The remote peer disconnected on us.  Treat this as an EOF.
      bytesRead = -1;
      goto done;
    }
    jthr = newSocketException(env, ret, "recvmsg(2) failed: %s",
                              terror(ret));
    goto done;
  } else if (bytesRead == 0) {
    bytesRead = -1;
    goto done;
  }
  jRecvFdsLen = (aux.hdr.cmsg_len - sizeof(struct cmsghdr)) / sizeof(int);
  for (i = 0; i < jRecvFdsLen; i++) {
    fdObj = fd_create(env, aux.fds[i]);
    if (!fdObj) {
      jthr = (*env)->ExceptionOccurred(env);
      (*env)->ExceptionClear(env);
      goto done;
    }
    // Make this -1 so we don't attempt to close it twice in an error path.
    aux.fds[i] = -1;
    (*env)->SetObjectArrayElement(env, jfds, i, fdObj);
    // There is no point keeping around a local reference to the fdObj.
    // The array continues to reference it.
    (*env)->DeleteLocalRef(env, fdObj);
  }
  (*env)->SetByteArrayRegion(env, jbuf, offset, length, flexBuf.curBuf);
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->ExceptionClear(env);
    goto done;
  }
done:
  flexBufFree(&flexBuf);
  if (jthr) {
    // Free any FileDescriptor references we may have created,
    // or file descriptors we may have been passed.
    for (i = 0; i < jRecvFdsLen; i++) {
      if (aux.fds[i] >= 0) {
        RETRY_ON_EINTR(i, close(aux.fds[i]));
        aux.fds[i] = -1;
      }
      fdObj = (*env)->GetObjectArrayElement(env, jfds, i);
      if (fdObj) {
        int ret, afd = fd_get(env, fdObj);
        if (afd >= 0) {
          RETRY_ON_EINTR(ret, close(afd));
        }
        (*env)->SetObjectArrayElement(env, jfds, i, NULL);
        (*env)->DeleteLocalRef(env, fdObj);
      }
    }
    (*env)->Throw(env, jthr);
  }
  return bytesRead;
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_readArray0(
JNIEnv *env, jclass clazz, jint fd, jarray b, jint offset, jint length)
{
  int ret = -1;
  struct flexibleBuffer flexBuf;
  jthrowable jthr;

  jthr = flexBufInit(env, &flexBuf, length);
  if (jthr) {
    goto done;
  }
  RETRY_ON_EINTR(ret, read(fd, flexBuf.curBuf, length));
  if (ret < 0) {
    ret = errno;
    if (ret == ECONNABORTED) {
      // The remote peer disconnected on us.  Treat this as an EOF.
      ret = -1;
      goto done;
    }
    jthr = newSocketException(env, ret, "read(2) error: %s",
                              terror(ret));
    goto done;
  }
  if (ret == 0) {
    goto done;
  }
  (*env)->SetByteArrayRegion(env, b, offset, ret, flexBuf.curBuf);
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->ExceptionClear(env);
    goto done;
  }
done:
  flexBufFree(&flexBuf);
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
  return ret == 0 ? -1 : ret; // Java wants -1 on EOF
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_available0(
JNIEnv *env, jclass clazz, jint fd)
{
  int ret, avail = 0;
  jthrowable jthr = NULL;

  RETRY_ON_EINTR(ret, ioctl(fd, FIONREAD, &avail));
  if (ret < 0) {
    ret = errno;
    jthr = newSocketException(env, ret,
              "ioctl(%d, FIONREAD) error: %s", fd, terror(ret));
    goto done;
  }
done:
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
  return avail;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_writeArray0(
JNIEnv *env, jclass clazz, jint fd, jarray b, jint offset, jint length)
{
  struct flexibleBuffer flexBuf;
  jthrowable jthr;

  jthr = flexBufInit(env, &flexBuf, length);
  if (jthr) {
    goto done;
  }
  (*env)->GetByteArrayRegion(env, b, offset, length, flexBuf.curBuf);
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->ExceptionClear(env);
    goto done;
  }
  jthr = write_fully(env, fd, flexBuf.curBuf, length);
  if (jthr) {
    goto done;
  }

done:
  flexBufFree(&flexBuf);
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_readByteBufferDirect0(
JNIEnv *env, jclass clazz, jint fd, jobject dst, jint position, jint remaining)
{
  uint8_t *buf;
  jthrowable jthr = NULL;
  int res = -1;

  buf = (*env)->GetDirectBufferAddress(env, dst);
  if (!buf) {
    jthr = newRuntimeException(env, "GetDirectBufferAddress failed.");
    goto done;
  }
  RETRY_ON_EINTR(res, read(fd, buf + position, remaining));
  if (res < 0) {
    res = errno;
    if (res != ECONNABORTED) {
      jthr = newSocketException(env, res, "read(2) error: %s",
                                terror(res));
      goto done;
    } else {
      // The remote peer disconnected on us.  Treat this as an EOF.
      res = -1;
    }
  }
done:
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
  return res;
}
