/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <iomanip>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);
	//add itself to its own membership table
	//we add port since there may be more than two processes on the
	//same host (ip address). In other words processes are uniquely identified
	//with <IP, PORT> pairs.
	memberNode->memberList.push_back(MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime()));
	log->logNodeAdd(&memberNode->addr, &memberNode->addr);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}


/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */

		// for (auto& me : memberNode->memberList)
		// {
		// 	Address addr = Mle2Address(&me);
		// 	log->logNodeRemove(&memberNode->addr, &addr);
		// }
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size )
{
	//extract the message type
	MessageHdr* msgHdr = (MessageHdr*)data;
	//then the address of the member node (that sent message, source)
	Address* sender = (Address*)(msgHdr + 1);

	switch(msgHdr->msgType)
	{
		case JOINREQ:
									//introducer accepts a node
									//cout << memberNode->addr.getAddress() << " is accepting " << sender->getAddress() << endl;
									acceptNode(sender);
									break;
		case JOINREP:
									//welcome!
									//cout << memberNode->addr.getAddress() << " became a member" << endl;
									memberNode->inGroup = 1;
									break;

		case HEARTBEAT:
									//sizeof(MessageHdr) + sizeof(Address) + 1
									// 	//structure of the message
									// 	// | msg-type | sender-addr | heartbeat | #entries in the list | list entries |
									// 	msg->msgType = HEARTBEAT;
									// 	memcpy((char*)(msg+1), &memberNode->addr, sizeof(memberNode->addr));
									// 	memcpy((char*)(msg+1) + sizeof(memberNode->addr) + 1, &memberNode->heartbeat, sizeof(long));
									// 	memcpy((char*)(msg+1) + sizeof(memberNode->addr) + 1 + sizeof(long), &num_elements, sizeof(size_t));
									// 	memcpy((char*)(msg+1) + sizeof(memberNode->addr) + 1 + 2*sizeof(long), me_array, list_size);
									{
										long* hb = (long*)(data + sizeof(MessageHdr) + sizeof(Address) + 1);
										size_t* lsize = (size_t*) (data + sizeof(MessageHdr) + sizeof(Address) + 1 + sizeof(long));
										char* list = (char*)(data + sizeof(MessageHdr) + sizeof(Address) + 1 + sizeof(long) + sizeof(size_t));

										//cout << memberNode->addr.getAddress() << " recieved hb from " << sender->getAddress();
										//cout << " of size " << size << ", msg: " << endl;
										//printBytes(list, *lsize * (sizeof(long) + sizeof(memberNode->addr.addr) ));

									  syncMembershipList(list, *lsize, *hb);
									}

									break;

		default: assert(false);
	};
}

void MP1Node::acceptNode(Address* sender)
{
	MessageHdr* msg;

  int msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr) + sizeof(long) + 1;
  msg = (MessageHdr *) malloc(msgsize*sizeof(char));
	assert(msg != NULL);
	//create JOINREP message from memberNode
  msg->msgType = JOINREP;
	memcpy((char*)(msg+1), &memberNode->addr, sizeof(memberNode->addr));
  memcpy((char*)(msg+1) + 1 + sizeof(memberNode->addr), &memberNode->heartbeat, sizeof(long));
	//and send the message to node
	emulNet->ENsend(&memberNode->addr, sender, (char*)msg, msgsize);
	free(msg);

	//add to members list so that to propagate to other nodes
	int id = *(int*)(&sender->addr);
	int port = *(short*)(&sender->addr[4]);
	memberNode->memberList.push_back(MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime()));
	log->logNodeAdd(&memberNode->addr, sender);
}


void MP1Node::syncMembershipList(char* list, size_t lsize, long hb)
{
	long* heartbeat;
	short* port;
	int* id;

	int block_size = sizeof(long) + sizeof(memberNode->addr.addr);

	for(int i = 0; i < lsize; i++ )
	{
		//printBytes((char*)(list + i*block_size), block_size);
		id = (int*)((char*)(list + i*block_size));
		port = (short*)((char*)(list + i*block_size) + sizeof(int));
		heartbeat = (long*)((char*)(list + i*block_size) + sizeof(int) + sizeof(short));
		updateMembershipList(*id, *port, *heartbeat);
	}
}

void MP1Node::updateMembershipList(int id, short port, long hb)
{
	for (auto& me : memberNode->memberList)
	{
		if ((me.getid() == id) && (me.getport() == port))
		{
			if (me.getheartbeat() < hb)
			{
		  	me.setheartbeat(hb);
				me.settimestamp(par->getcurrtime());
			}
			return;
		}
	}
	//not found, add a new entry
	memberNode->memberList.push_back(MemberListEntry(id, port, hb, par->getcurrtime()));
	MemberListEntry e(id, port);
	Address addr = Mle2Address(&e);
	log->logNodeAdd(&memberNode->addr, &addr);
}

Address MP1Node::Mle2Address(MemberListEntry* entry)
{
	Address address;
	memcpy(address.addr, &entry->id, sizeof(int));
	memcpy(&address.addr[4], &entry->port, sizeof(short));
	return address;
}

void MP1Node::removeNodes()
{
	vector<MemberListEntry> failedNodes, liveNodes;

	for (auto& me : memberNode->memberList)
	{
		Address addr = Mle2Address(&me);
		if (memberNode->addr == addr)
		{
			liveNodes.push_back(me);
			continue;
		}

		if (par->getcurrtime() - me.timestamp > TFAIL)
			failedNodes.push_back(me);
		else
		  liveNodes.push_back(me);
	}

	memberNode->memberList = liveNodes;

	for (auto& me : failedNodes)
	{
		Address addr = Mle2Address(&me);
		//cout << memberNode->addr.getAddress() << " removes: " << addr.getAddress() << endl;
		log->logNodeRemove(&memberNode->addr, &addr);
	}

}
/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps()
{
	removeNodes();

	//increase your heartbeat and
	++memberNode->heartbeat;

	//update your membership list
	for (auto& me : memberNode->memberList)
	{
		Address addr = Mle2Address(&me);
		if (memberNode->addr == addr)
		{
			//cout << memberNode->addr.getAddress() << "'s HB: " << memberNode->heartbeat << endl;
			me.setheartbeat(memberNode->heartbeat);
			me.settimestamp(par->getcurrtime());
			break;
		}
	}

	//propagate your membership list
	propagateMembershipList();

  return;
}


void MP1Node::propagateMembershipList()
{
	MessageHdr* msg;

	//serialize membership list
	size_t num_elements = memberNode->memberList.size();
	size_t list_size = (sizeof(long) + sizeof(int) + sizeof(short)) * num_elements;

	char* me_array = (char*)malloc(list_size);
	assert(me_array != NULL);
	serializeMembershipList(me_array);

	//create a message
  int msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr) + sizeof(long) + 1 + sizeof(size_t) + list_size;
  msg = (MessageHdr*) malloc(msgsize);
	assert(msg != NULL);

	//structure of the message
	// | msg-type | sender-addr | heartbeat | #entries in the list | list entries |
  msg->msgType = HEARTBEAT;
	memcpy((char*)(msg+1), &memberNode->addr, sizeof(memberNode->addr));//sender
  memcpy((char*)(msg+1) + sizeof(memberNode->addr) + 1, &memberNode->heartbeat, sizeof(long));
	memcpy((char*)(msg+1) + sizeof(memberNode->addr) + 1 + sizeof(long), &num_elements, sizeof(size_t));
	memcpy((char*)(msg+1) + sizeof(memberNode->addr) + 1 + sizeof(long) + sizeof(size_t), me_array, list_size);

	//send the message to all nodes
	for (auto& me : memberNode->memberList)
	{
		Address addr = Mle2Address(&me);
		//cout << memberNode->addr.getAddress() << " is sending heartbeat to " << addr.getAddress() << " of size " << msgsize << ", msg: " <<  endl;
		//printBytes((char*)(msg+1) + sizeof(memberNode->addr) + 1 + sizeof(long) + sizeof(size_t), list_size);
		emulNet->ENsend(&memberNode->addr, &addr, (char*)msg, msgsize);
	}

	free(me_array);
	free(msg);
}



void MP1Node::serializeMembershipList(char* me_array)
{
	long hb;
	int id, offset = 0;
	int block_size = sizeof(int) + sizeof(short) + sizeof(long);
	short port;

	//cout << "list in serl: ";
	for (auto& me : memberNode->memberList)
	{
		hb = me.getheartbeat();
		id = me.getid();
		port = me.getport();

		memcpy((char*)(me_array + offset), &id, sizeof(int));
		memcpy((char*)(me_array + offset) + sizeof(int), &port, sizeof(short));
		memcpy((char*)(me_array + offset) + sizeof(int) + sizeof(short), &hb, sizeof(long));
		//cout << me_array[offset].address << ", " << me_array[offset].heartbeat;
		offset += block_size;
	}
}


/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}

void MP1Node::printBytes(char* arr, size_t sz)
{
	cout << "  ";
	for (int i=0; i<sz; i++)
	{
		cout << std::hex << std::setfill('0') << std::setw(2) << (unsigned int)arr[i] << " " << std::dec;
	}
	cout << " hb: " << *((long*)(arr + sizeof(memberNode->addr.addr)));
	cout << endl;
}
