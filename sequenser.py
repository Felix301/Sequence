import sys
import socket
import threading
import thread
import pickle
import os
import random
import time

Vclock={1:0,2:0,3:0,4:0}
sequence=0
hold_back_list = []

ADDRESS=('localhost',8080)
g_conn_pool=[]
server = None
server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  
server.bind(ADDRESS)
client=set()      

print("Sequenser start running")

def wrap_message(message,seq_num,sender_id):
    dict = {"message_contents": message, "seq_num": seq_num,"sender_id": sender_id}
    return pickle.dumps(dict)

def send_message(message,seq_num,sender_id):
    for n in client:
        server.sendto(wrap_message(message,seq_num,sender_id), n)
        
    
def receive_message(message_from_client,address):
    thread = threading.Thread(target = run, args=(message_from_client,))
    #add address when receving instead of initializion
    if address not in client: 
        client.add(address)
    thread.start() 

def run(message_from_client):
    global Vclock
    global sequence
    delay=random.random()*5  
    time.sleep(delay)    #stimulate delay
    print("receive message from id:%d,with LC:%d at %f"%(message_from_client["sender_id"],message_from_client["local_clock"],time.time()))  #for check purpose
    
    if message_from_client["local_clock"] == Vclock[message_from_client["seq_num"]]:
        Vclock[message_from_client["sender_id"]]+=1
        sequence+=1
        send_message(message_from_client["message_contents"],sequence,+message_from_client["sender_id"])
        check=-1
        while (check < 0):
            for n in hold_back_list:
                check+=1
                if (message_from_client["sender_id"] == n["sender_id"]):
                    if(Vclock[message_from_client["sender_id"]] == n["local_clock"]):
                        send_message(n["message_contents"],sequence+1,n["sender_id"])
                        print("send message:%s with sequence:%d,sender_id:%d localclock:%d"%(n["message_from_client"],sequence+1,n["sender_id"],n["local_clock"]))
                        Vclock[message_from_client["sender_id"]]+=1
                        sequence+=1
                        check-=len(hold_back_list)
                        hold_back_list.remove(n)
        
    else:
        hold_back_list.append(message_from_client)
        
if __name__ == '__main__':
    message,address = server.recvfrom(1024)
    receive_message(pickle.loads(message),address)
    
    
    
    
    
    
    
