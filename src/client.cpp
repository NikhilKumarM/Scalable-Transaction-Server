/*

    Author : Nikhil Kumar Mengani
    This is client.


*/

#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<errno.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<unistd.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<iostream>  
#include<fstream>   
#include<sstream>   


using namespace std;

#define BUFFER_SIZE 255



/*

     main method of client reads the transactions present in the file 
     one by one and sends them to the server. client waits till it receives 
     the acknowledge from the server.
*/

int main(int argc ,char** argv)
{

       int client;
       struct sockaddr_in  server_address;
       char  *input;
       char *output;
       ifstream   infile;
       infile.open(argv[4]);
       //cout<<  "This is client"<< endl;
       char buffer[BUFFER_SIZE];
       float transaction_delay =atof( argv[3]);
   
       //server address  
       server_address.sin_family = AF_INET;
       server_address.sin_port = htons(atoi(argv[2]));
       server_address.sin_addr.s_addr =inet_addr(argv[1]);
       bzero(&server_address.sin_zero,8);
       memset(buffer,'\0',sizeof(buffer));

       // reading the each record in the transactions.txt file
      
       for(string line;getline(infile,line);)
         {
            
            strcpy(buffer,line.c_str());     
            // creating the socket to connect to server
            client = socket(AF_INET, SOCK_STREAM,0);                
              
            if(client==-1)
              {
 
                   perror("Error while creating the client socket");
                   exit(-1);
              }

            // connecting to the server
            if(connect(client, (struct sockaddr*)&server_address,sizeof(struct sockaddr_in))==-1)
              {

                  perror("error while connecting to server");
                  exit(-1);
                }
           
            int sent = send(client,buffer,sizeof(buffer),0);  
            memset(buffer,'\0',sizeof(buffer));
            char msg[200];
            memset(msg,'\0',sizeof(msg));
            // waiting for the acknowledge from the server
            int received = recv(client,msg,sizeof(msg),0);
            cout<< "MESSAGE FROM SERVER : "<<msg<<endl; 
            close(client);
            usleep(transaction_delay*1000000);
                  
         }
        
}



