//
// Created by Chess on 2021/1/15.
//
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <strings.h>
#include <string.h>
#include <arpa/inet.h>

#define PORTNUM 5052
#define BUFSIZE 4096
#define oops(msg) {perror(msg);}

int ueba_client(){
    struct sockaddr_in saddr;
    int sock_id, sock_fd;
    char message[BUFSIZ];
    int messlen;

    /**
     * get a socket
     */
     sock_id = socket(AF_INET, SOCK_STREAM, 0);
     if (sock_id == -1)
         oops("socket");

     saddr.sin_port = htons(PORTNUM);
     saddr.sin_family = AF_INET;
     inet_aton("127.0.0.1", &saddr.sin_addr);

     if (connect(sock_id, (struct sockaddr*)&saddr, sizeof(saddr)) != 0)
         oops("connect");

     // messlen = read(sock_id, message, BUFSIZE);
     strcpy(message, "hello from ueba client");
     messlen = strlen(message);
     if (messlen == -1)
         oops("read");
     if (write(sock_id, message, messlen) != messlen)
         oops("write");
     close(sock_id);
}

int main(){
    ueba_client();
}