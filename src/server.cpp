/*    
      Author : Nikhil Kumar Mengani
      this is server  is server code  
*/


#include<stdio.h>
#include<stdlib.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<errno.h>
#include<string.h>
#include<unistd.h>
#include<arpa/inet.h>
#include<iostream>
#include<pthread.h>
#include<sstream>
#include<queue>
#include<fstream>
#include<map>
#include <iomanip>

#define QUEUE_SIZE 1000
#define MAX_CLIENTS 100
#define THREAD_POOL  20


using namespace std;

// global variables

queue<int> myqueue;
map<string, int> mymap;
fstream afile, sfile;
pthread_mutex_t locks[100000];   
pthread_mutex_t queue_lock;
pthread_t clients[20];
pthread_cond_t not_full, not_empty;
int lock_no=0;
char cur_record[128];
int total_count=0;
unsigned int requests=0; 

/*

   This function allocates the unique number to each record. This number is used 
   to lock on specific record. 


*/

void client_lock_map(string fname)
       {
          afile.open(fname.c_str(), ios::out | ios::in );
          for(string line;getline(afile,line);)
          {     
                istringstream iss(line);
                string account_no ;
                int i=0;
           
                while(getline(iss,account_no,' '))
                 {
                     if(i==0)
                     {
                      mymap[account_no] = lock_no;
                      lock_no++;
                      break;
                     }
                       
                }
          }
          afile.clear();  
          afile.seekg(0,ios::beg);
          ofstream change_file;
          change_file.open("./temp_file.txt");
          for(string line;getline(afile,line);)
          {
                 memset(cur_record,' ',sizeof(cur_record));
                 strcpy(cur_record, line.c_str());  
                 change_file<<cur_record;
                 for(int i=0;i<128-strlen(cur_record);i++)
                  {

                           change_file<<' ';

                  }      
                 change_file<<'\n';
          }  
           change_file.close(); 
           ifstream c_file; 
           afile.close(); 
      }



/*

   This method is a thread which handles the each request. It recieves the request from the client and
   upon completion of the transaction sends back the acknowledgement back to client. 

*/

void *thread_pooling(void * args)
      { 
        
        while(1)
        {
                 
                 pthread_mutex_lock(&queue_lock);
                 while(myqueue.empty())
                        {
                 // waiting for the signal saying queue has the request
                        pthread_cond_wait(&not_empty,&queue_lock);
                        }            
                 int sock_ = myqueue.front();
                 myqueue.pop();
                 // signalling that request is consumed and queue is not empty   
                 pthread_cond_signal(&not_full);
                 pthread_mutex_unlock(&queue_lock);
                 char buffer[255];
                 int data_len,lock_number;
                 string time_stamp,account_no,operation,amount;
                 memset(buffer,'\0',sizeof(buffer));
                 // receiving the transaction request from the client
                 data_len = recv(sock_,buffer,sizeof(buffer),0 );
                 
	         if(data_len!=0)
                 { 
                      string record_item;  
                      int i=0; 
                      istringstream iss(buffer);
                      while(getline(iss,record_item,' ')) 
                         { 
                           i++;
                           if(i==1)
                             {
		                time_stamp = record_item;
                             }
		            else if(i==2)
		             {
                                account_no = record_item; 
                                lock_number = mymap[record_item];
		             }
		            else if(i==3)
		            {       
		                operation = record_item;

		             }
		            else
		            {
                                amount = record_item;  
		            }
                        }
             
                  }
                    
         
      
            
                    string acc_no,original_bal;
                    double balance;
                    double withd_depos = atof(amount.c_str()); 
                    string msg;
                    int status_code;
                    fstream sfile;
                    sfile.open("./temp_file.txt",ios::out| ios::in); 

                    // scanning for the account number upon which transaction has to be performed 

                    for(string line;getline(sfile,line);)
                    {     
                         istringstream iss7(line);
                         string rec_item;
                         streampos shift=128;
                         int p=0;
                         while(getline(iss7,rec_item,' '))
                         {
                               if(p==0)
                               {
                                 acc_no = rec_item;
                                 break;                  
                                }
                              
                         }
              
                         if(strcmp(acc_no.c_str(), account_no.c_str())==0)
                            {
                                 // account number found and start processing of the request
   
                                 pthread_mutex_lock(&locks[lock_number]);
                                 streampos cur_pos1 = sfile.tellg();
                                 sfile.seekp(cur_pos1-shift);
                                 getline(sfile,line); 
                                 istringstream iss1(line);
                                 string rec; 
                                 int c=0;
                                 while(getline(iss1,rec,' '))
                                 {
                                     if(c==2)
                                    {
                              
                                      balance = atof(rec.c_str());
                                      original_bal= rec;
                           
                                     }
                                    c++; 
                                 }  
                                string oper1 = "w";  
                                if(strcmp(operation.c_str(),oper1.c_str())==0)
		                {
                                      cout << "TRANSACTION REQUESTED BY CLIENT: " << "Withdraw the amount $"<< amount << 
                                              " from account number  " << account_no<<endl;
                                      
                                      if(withd_depos<=balance)
			              {

                                            balance = balance - withd_depos;
                                            ostringstream oss2;
                                            oss2<< "Amount: $"<< withd_depos<<" withdrawn from the account."<<
                                                    account_no <<" Balance : "<<setprecision(3)<<showpoint<<fixed<< balance;
                                            msg = oss2.str();    
                                            
             			      }
                                     else        
                                      {
                                    
                                             cout<< "not enough balance."<<endl;
                                             msg = "Not enough balance";
                                              
                                       }
                                       
                      
                                }
                               string oper2="d";
                               if(strcmp(operation.c_str(),oper2.c_str())==0)
                                {

                                         
                                         cout << "TRANSACTION REQUESTED BY CLIENT: " << "Deposit the amount $" << amount  <<
                                                 " to the account number "+ account_no<<endl;
                                    
                                         balance = balance+withd_depos;
                                         ostringstream oss3;
                                         oss3<< "Amount: $"<< withd_depos<<" deposited into the account."<< account_no <<
                                                " Balance : "<<setprecision(3)<<showpoint<<fixed<< balance;
                                         msg = oss3.str();     
                                        

                                }

                              int count=0,h=0;
		              for(h=0;h<line.length();h++)
			       {
                                        if(line[h]==' ')
			                {

                                          count++;
			                 }
                                         if(count==2)
			                 {
                                         break;

			                 }

			       }

			     ostringstream oss;
			     oss<<setprecision(3)<<showpoint<<fixed<<balance;
			     string updated_bal = oss.str();
                             int j=0;
                             for(j=0;j<updated_bal.length();j++)
                             {
                                     
                                     line[h+1+j] = updated_bal[j];
 
                             }
                        
                             if(original_bal.length()>updated_bal.length())
                             {        
                                   for(int k=0;k<(original_bal.length()-updated_bal.length());k++)
                                   {
                                      line[h+1+j+k] =' ';
                                   } 

                             }
                       
                             streampos cur_pos = sfile.tellg();
                             int line_len = line.length();
                             sfile.seekp(cur_pos-shift); 
                             sfile<< line;
      	                     sfile.close();
                             total_count ++;
		             // sending the transaction message to the client
                             int sent = send(sock_,msg.c_str(),200,0);                          
                             close(sock_);
                             pthread_mutex_unlock(&locks[lock_number]);
                  
                     }
                   


                  } 
       
            }
       return NULL;


      }



/*


      main method starts the server and waits for the requests from the client. I am maintaining  
      a queue in which I am inserting the socketid's of the requests from the clients. Threads will
      process these requests poping the the socketid's. Here the queue acts as producer and threads   
      acts as consumer


*/

int  main(int argc , char** argv)
{
       
       int server_socket,client_socket;
       struct sockaddr_in server_address, client_address;
       unsigned  int length;

      
       cout << "This is server"<<endl;
       client_lock_map(argv[2]);
       server_socket = socket(AF_INET, SOCK_STREAM,0);
       if(server_socket==-1)
       {
             perror("Error creating server socket");
	     exit(-1);
        }

	server_address.sin_family = AF_INET;
	server_address.sin_port = htons(atoi(argv[1]));
	server_address.sin_addr.s_addr = INADDR_ANY;
        bzero(&server_address.sin_zero,8);
        length = sizeof(struct sockaddr_in);
	
        // binding the socket to address and port
	
        if((bind(server_socket,(struct sockaddr*)&server_address,length))==-1)
	{
                 perror("Error binding to the server socket");
		 exit(-1);
        }

        if(listen(server_socket,MAX_CLIENTS)==-1)
	  {
              perror("error while listening");
	      exit(-1);

	  }
        
 /* 
            
            using the concept thread pooling. Starting certain constant number of threads at the 
            start of the server. Maintaining a queue to accept the request.These threads wait till the 
            requests are accepted and inserted into the queue. As soon as queue has a request, one of 
            the threads will process it.  
            
 */
        
         for(int i=0;i< THREAD_POOL;i++)
         {
         int  err= pthread_create(&clients[i],NULL,&thread_pooling,NULL);
                  
         }
        
         pthread_mutex_init(&queue_lock,NULL);
         pthread_cond_init(&not_full,NULL);
         pthread_cond_init(&not_empty,NULL);
  
         while(1)
	 {    
            // accepting the client requests
            client_socket = accept(server_socket,(struct sockaddr*)&client_address,&length);
             if( client_socket==-1)
	            {
                      perror("error server connecting to client");
	           exit(-1);
	       }             
            pthread_mutex_lock(&queue_lock);   
           
            while(myqueue.size()== QUEUE_SIZE)
             {       
             //waiting for the thread to consume and make space for the queue 
                    pthread_cond_wait(&not_full,&queue_lock);
             }
             
            
            requests++;
            //cout<< "Total requests: "<< requests <<endl;
            myqueue.push(client_socket);
            // signalling the thread that queue is not empty and to process the request
            pthread_cond_signal(&not_empty); 
            pthread_mutex_unlock(&queue_lock);
            

         }

          close(server_socket);
          return 0;
}

  

