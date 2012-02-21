/*************************************************************************************************
 * System-dependent configurations of Tokyo Tyrant
 *                                                               Copyright (C) 2006-2010 FAL Labs
 * This file is part of Tokyo Tyrant.
 * Tokyo Tyrant is free software; you can redistribute it and/or modify it under the terms of
 * the GNU Lesser General Public License as published by the Free Software Foundation; either
 * version 2.1 of the License or any later version.  Tokyo Tyrant is distributed in the hope
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * You should have received a copy of the GNU Lesser General Public License along with Tokyo
 * Tyrant; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307 USA.
 *************************************************************************************************/


#include "myconf.h"



/*************************************************************************************************
 * common settings
 *************************************************************************************************/


int _tt_dummyfunc(void){
  return 0;
}


int _tt_dummyfuncv(int a, ...){
  return 0;
}



/*************************************************************************************************
 * epoll emulation
 *************************************************************************************************/


#if defined(TTUSEKQUEUE)


#include <sys/event.h>


int _tt_epoll_create(int size){
  return kqueue();
}


int _tt_epoll_ctl(int epfd, int op, int fd, struct epoll_event *event){
  if(op == EPOLL_CTL_ADD || op == EPOLL_CTL_MOD){
    struct kevent kev;
    int kfilt = 0;
    if(event->events & EPOLLIN){
      kfilt |= EVFILT_READ;
    } else {
      fprintf(stderr, "the epoll emulation supports EPOLLIN only\n");
      return -1;
    }
    if(event->events & EPOLLOUT){
      fprintf(stderr, "the epoll emulation supports EPOLLIN only\n");
      return -1;
    }
    int kflags = EV_ADD;
    if(event->events & EPOLLONESHOT) kflags |= EV_ONESHOT;
    EV_SET(&kev, fd, kfilt, kflags, 0, 0, NULL);
    return kevent(epfd, &kev, 1, NULL, 0, NULL) != -1 ? 0 : -1;
  } else if(op == EPOLL_CTL_DEL){
    struct kevent kev;
    int kfilt = EVFILT_READ;
    EV_SET(&kev, fd, kfilt, EV_DELETE, 0, 0, NULL);
    return kevent(epfd, &kev, 1, NULL, 0, NULL) != -1 || errno == ENOENT ? 0 : -1;
  }
  return -1;
}


int _tt_epoll_reassoc(int epfd, int fd){
  return 0;
}


int _tt_epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout){
  div_t td = div(timeout, 1000);
  struct timespec ts;
  ts.tv_sec = td.quot;
  ts.tv_nsec = td.rem * 1000000;
  struct kevent kevs[maxevents];
  int num = kevent(epfd, NULL, 0, kevs, maxevents, &ts);
  if(num == -1) return -1;
  for(int i = 0; i < num; i++){
    events[i].data.fd = kevs[i].ident;
  }
  return num;
}


#elif defined(TTUSEEVPORTS)


#include <port.h>


int _tt_epoll_create(int size){
  int port = port_create();
  if(port < 0) return -1;
  return port;
}


int _tt_epoll_ctl(int epfd, int op, int fd, struct epoll_event *event){
  if(op == EPOLL_CTL_ADD || op == EPOLL_CTL_MOD){
    if(event->events & EPOLLIN){
      int result = port_associate(epfd, PORT_SOURCE_FD, fd, POLLIN, event);
      if(result == -1) return -1;
      return 0;
    } else {
      return -1;
    }
  } else if(op == EPOLL_CTL_DEL){
    int result = port_dissociate(epfd, PORT_SOURCE_FD, fd);
    if(result == -1) return -1;
    return 0;
  }
  return -1;
}


int _tt_epoll_reassoc(int epfd, int fd){
  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = fd;
  if(epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) != 0) return -1;
  return 0;
}


int _tt_epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout){
  div_t td = div(timeout, 1000);
  struct timespec ts;
  ts.tv_sec = td.quot;
  ts.tv_nsec = td.rem * 1000000;
  port_event_t list[maxevents];
  unsigned int actevents = maxevents;
  if(port_getn(epfd, list, 0, &actevents, &ts) == -1) return -1;
  if(actevents == 0) actevents = 1;
  if(actevents > maxevents) actevents = maxevents;
  if(port_getn(epfd, list, maxevents, &actevents, &ts) == -1){
    if(errno == EINTR){
      return 0;
    } else if(errno != ETIME){
      return -1;
    }
  }
  for(int i = 0; i < actevents; i++){
    events[i].data.fd = list[i].portev_object;
  }
  return actevents;
}


#endif



// END OF FILE
