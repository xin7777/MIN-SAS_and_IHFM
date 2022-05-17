//
// Created by Chess on 2021/1/15.
//
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <strings.h>
#include <arpa/inet.h>

#define PORTNUM 5052
#define BUFSIZE 4096
#define oops(msg) {perror(msg);}

void ueba_producer(){
    struct sockaddr_in saddr;
    int sock_id, sock_fd;
    FILE *sock_fp, *sock_fp2;
    char message[BUFSIZE];

    /*
     * ask for a socket
     */
    sock_id = socket(PF_INET, SOCK_STREAM, 0);
    if (sock_id == -1)
        oops("socket")

    /**
     * bind address to socket
     */

     saddr.sin_port = htons(PORTNUM);
     saddr.sin_family = AF_INET;
     inet_aton("127.0.0.1", &saddr.sin_addr);


     if (bind(sock_id, (struct sockaddr *)&saddr, sizeof(saddr)) != 0)
         oops("bind");

     if(listen(sock_id, 10) != 0)
         oops("listen");

     while (1) {
         sock_fd = accept(sock_id, NULL, NULL);
         fprintf(stdout, "RECEIVED\n");
         if (sock_fd == -1)
             oops("accept");
         sock_fp = fdopen(sock_fd, "r");
         sock_fp2 = fdopen(sock_fd, "w");
         if (sock_fp2 == NULL || sock_fp == NULL)
             oops("fdopen");

         fgets(message, BUFSIZE, sock_fp);
         printf("message:%s\n", message);
         fputs("RECEIVED", sock_fp2);
         fclose(sock_fp);
         fclose(sock_fp2);
     }
}

int main(){
    ueba_producer();
}