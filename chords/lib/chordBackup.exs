import GenerateRandomStrings
import Hash
import Common

defmodule Chords1 do
    @moduledoc """
    Chord is a protocol and algorithm for a peer-to-peer distributed hash table. 
    A distributed hash table stores key-value pairs by assigning keys to different computers (known as "nodes"); a node will store the values for all the keys for which it is responsible. 
    Chord specifies how keys are assigned to nodes, and how a node can discover the value for a given key by first locating the node responsible for that key.
    """

    @doc """
    Generate random string based on the given legth. It is also possible to generate certain type of randomise string using the options below:
    @param :numNodes -  the number of peers to be created in the peer to peer system 
    @param :numRequests - the number of requests each peer has to make.
    When all peers performed that many requests, the program can exit.
    Each peer shouldsend a request/second.
    ## Example
        iex> Chords.main(1000, 20) //"The average number of hops (node connections) that have to be traversed to deliever a message is 256."
    """
    def main do
        if (Enum.count(System.argv())!=2) do
            IO.puts" Illegal Arguments Provided"
            System.halt(1)
        else
            [numNodes, numRequests] = System.argv();
            {numNodes, _} = Integer.parse(numNodes);
            {numRequests, _} = Integer.parse(numRequests);
            if numNodes > :math.pow(2, 256) do
                IO.puts("Number  of nodes should be less that 2^256")
            else 
                if numNodes <= 0 do
                    IO.puts("Number of nodes should be positive")
                else
                    if numRequests <= 0 do
                        IO.puts("Number of nodes should be positive")
                    else
                        allNodesMap = createNodes(numNodes, numRequests)
                        pidHashMap = List.keysort(allNodesMap, 1)
                        allKeys = createKeys(numNodes)
                        buildRing(pidHashMap)
                        assignKeysToNodes(allKeys, pidHashMap)
                        createFingerTable(pidHashMap, numNodes)
                    end
                end
            end
        end
    end

    @doc """
    Creates <numNodes> Nodes, i.e. Processes. We collect all the PIDs of these processes and hash them. Finally we return a list of PIDs and their respective hashes. Arguments are as follows:
    @param :numNodes -  the number of peers to be created in the peer to peer system 
    @param :numRequests - the number of requests each peer has to make.
    ## Example
        iex> Chords.createNodes(2) 
        //Output
        [
            {#PID<0.122.0>, "1A2EF8ADECC2BB0CF46A7E192A015C371C9D2B4902986205D0DABDCA98D431D7"},
            {#PID<0.124.0>, "77C54B3D07894668A8B46606860276204E95BE4F3172A1A8A697D195B2358AE5"}
        ]
    """
    def createNodes(numNodes, numRequests) do
        allHashedNodes = Enum.map((1..numNodes), fn(x) ->
            pid=start_node()
            pidStr  = Kernel.inspect(pid)
            hashPid = generateHash(pidStr)
            updatePIDState(pid, hashPid)
            updateRequestState(pid, numRequests)
            {pid, hashPid}
        end)
        allHashedNodes
    end

    @doc """
    create (2 * <numNodes>) random keys using GenerateRandomStrings module. Arguments are as follows:
    @param numNodes -  the number of peers to be created in the peer to peer system 
    ## Example
        iex> Chords.createKeys(2) 
        //Output
        ["4Le7C", "WKW2g", "TteAa", "kXi4L"]
    """
    def createKeys(numNodes) do
        allKeys = Enum.map((1..2*numNodes), fn(x) ->
            randomizer(5)
        end)
        # IO.inspect allKeys
        allKeys
    end

    @doc """
    create a new Chord ring (also called  identifier circle).
    All nodes are arranged in a ring topology, where each nodes stores the HashedPID of its successor.
    Main features of Chord are
    • Load balancing via Consistent Hashing
    • Small routing tables: log n
    • Small routing delay: log n hops
    • Fast join/leave protocol (polylog time)
    The argument is as follows:
    @param pidHashMap -  {PID, hashedPID} list Sorted on hashedPIDs
    """
    def buildRing(pidHashMap) do
        Enum.map(0..length(pidHashMap)-1, fn(x) ->
            pid = elem(Enum.fetch!(pidHashMap, x), 0)
            if x == length(pidHashMap)-1 do
                hashSuccesor = elem(Enum.fetch!(pidHashMap, 0), 1)
                updateSuccesorState(pid,hashSuccesor)
            else
                # hashSuccesor = elem(Enum.fetch!(pidHashMap, x+1), 1)
                # updateSuccesorState(pid,hashSuccesor)
                succesor = elem(Enum.fetch!(pidHashMap, x+1), 0)
                updateSuccesorState(pid,succesor)
            end
        end)

    end

    @doc """
    To avoid the linear search above, Chord implements a faster search method by requiring each node to keep a finger table containing up to m entries, recall that m is the number of bits in the hash key. 
    The i^{th} entry of node n will contain successor((n+2^{i-1}),mod,2^m). 
    The first entry of finger table is actually the node's immediate successor (and therefore an extra successor field is not needed). 
    Every time a node wants to look up a key k, it will pass the query to the closest successor or predecessor (depending on the finger table) of  k in its finger table (the "largest" one on the circle whose ID is smaller than  k), until a node finds out the key is stored in its immediate successor.
    With such a finger table, the number of nodes that must be contacted to find a successor in an N-node network is  O(log N). 
    """
    def createFingerTable(pidHashMap, numNodes) do
        m = round :math.floor(:math.log2(numNodes))
        Enum.map(0..length(pidHashMap)-1, fn(n) ->
            currentnode = Enum.fetch!(pidHashMap, n)
            Enum.each(0..m-1, fn(i) -> 
                nextFinger = calcfinger(currentnode, i)
                if elem(Enum.fetch!(pidHashMap, length(pidHashMap)-1), 0) < nextFinger do
                    successor = Enum.fetch!(pidHashMap, 0)
                    # map = %{"key": nextFinger, "value": successor }
                    map = %{"nextFinger": nextFinger, "successor": successor }
                    updateFingersState(elem(currentnode,0), map)
                else
                    IO.puts "greater condition"
                    result = Enum.map(0..length(pidHashMap)-1, fn(x) -> 
                        element = Enum.fetch!(pidHashMap, x)
                        if  nextFinger < elem(element,1) do
                            x
                        end
                    end)

                    # IO.inspect result
                    successor = Enum.fetch!(pidHashMap, Enum.fetch!(Enum.reject(result, &is_nil/1),0))
                    map = %{"nextFinger": nextFinger, "successor": successor}
                    updateFingersState(elem(currentnode,0), map)
                end
            end)
        end)
       
    end

    @doc """
    Returns computed key for finger k
    @param k - from 0 to (m - 1)
    """
    def calcfinger(currentnode, k) do
        intHashNodeId = elem(Integer.parse(elem(currentnode, 1), 16), 0)
        floorK = round :math.floor( 255 / (m - k + 1))
        powerOfTwo = Kernel.trunc(:math.pow(2,floorK))
        nextFinger = Integer.to_string(intHashNodeId + powerOfTwo, 16)
        nextFinger
    end

    @doc """
    Returns the node responsible for finger k
    @param m: Id length of the ring. (m = Key.idlength)
        Ring is constituted of 2^m nodes maximum
    """
    def lookupfinger(k, currentnode, useOnlySucc \\ false) do
        lookup(calcfinger(currentnode, k), currentnode, useOnlySucc)
    end

    @doc false
    def lookup(key, currentnode, useOnlySucc \\ false) do
        intHashNodeId = elem(Integer.parse(elem(currentnode, 1), 16), 0)
        {currentnodeHashId, successor, fingers, keys, numRequests} = getState(elem(currentnode, 0))
        # lookup on successor and then ask to the successor
        if useOnlySucc do
            # currentnode is successor ?
            if intHashNodeId == key do
                # result = currentnode
                intHashNodeId
            else
        
                # Is currentnode's successor the successor of key ? 
                successorHashId = elem(successor, 1)
                if isbetween(key, currentnodeHashId, successorHashId) do
                    elem(successor, 1)
                else
                    lookup(key, successor, useOnlySucc)
                end
            end

        # Use finger table to optim lookup
        else

            nfinger = length(fingers)
            fingmax = nfinger - 1

            # test if key to lookup is outside of finger tables
            if isbetween(key, elem(fingers, fingmax)[:nextFinger], elem(fingers, 0)[:successor]) do
                # let's ask to last finger
                # IO.puts("lookup recurse to node #{elem(fingers, 0)["resp"]}")
                lookup(elem(fingers, fingmax)[:successor], key, useOnlySucc)
            end
            
        end
    end

    @doc """
    UPdate finger table for all ring in a clockwise around successor fashion
    finger is an array of dict {resp, key}
        `resp` is the Node responsible for `key`
    @param newnode: new node which imply this update
    @param firstnode: node which launch the update
    """
    def updatefinger(currentnode, newnode, firstnode, numNodes) do
        m = round :math.floor(:math.log2(numNodes))
        Enum.each(0..m-1, fn(i) -> 
        
        # for i in range(0, self.uid.idlength) 
            fingerkey = calcfinger(i)
            resp = lookup(fingerkey, useOnlySucc=True)
            # map = %{"resp": resp, "key": fingerkey}
            map = %{"nextFinger": nextFinger, "successor": successor }
            updateFingersState(elem(currentnode,0), map)
        end)
        if elem(firstnode, 1) != elwm(successor, 1) do
            updatefinger(successor, newnode, firstnode)
        end
    end

    @doc """
    Assign  a key to the node when hash of key is just less that hash of PID of the node.
    Key k is assigned to the first node whose key is ≥ k (called the successor node of key k)
    Arguments are as follows:
    @param allKeys -  list of all keys to be stored in the peer-to-peer system
    @param pidHashMap -  list of {PID, hashedPID} Sorted on hashedPIDs
    """
    def assignKeysToNodes(allKeys, pidHashMap) do
        Enum.each(allKeys, fn(key) -> 
            hashedKey = generateHash(key)
            if hashedKey > Enum.fetch(pidHashMap, length(pidHashMap)-1) do
                succesor =  Enum.fetch!(pidHashMap, 0)
                updateSuccesorState(elem(succesor, 0), key)
                IO.puts "+++++++++++++++++++"
                IO.inspect result
            else
                count = 0
                allHashedNodes = []
                
                result = Enum.map(0..length(pidHashMap)-1, fn(x) -> 
                    element = Enum.fetch(pidHashMap, x)
                    if hashedKey < elem(elem(element,1), 1) do
                        x
                    end
                end)
                IO.puts "88888888888888"
                IO.inspect result
                successor = Enum.fetch!(pidHashMap, Enum.fetch!(Enum.reject(result, &is_nil/1),0))
                updateSuccesorState(elem(succesor, 0), key)
            end
            
        end)
        
    end

    @doc """
    Initiates the node with some default values as mentioned below:
    * Hashed PID 
    * Hashed PID of it's successor
    * List of Fingers of the node in the Chord Ring
    * List of Keys stored
    * Number of request a node has to send yet
    """
    def init(:ok) do
        {:ok, {'', '', [], [], 0}}
    end

    @doc """
    Spawns a new Genserver process and returns the PID
    ## Example
        iex> Chord.start_node()
        Output: 
        #PID<0.122.0>
    """
    def start_node() do
        {:ok,pid}=GenServer.start_link(__MODULE__, :ok,[])
        pid
    end

    @doc """
    Updates the successor of the node with PID <pid>
    """
    def updateSuccesorState(pid, succesor) do
        GenServer.call(pid, {:UpdateSuccesorState,succesor})
    end

    @doc false
    defp handle_call({:UpdateSuccesorState,succesor}, _from ,state) do
        {a,b,c,d,e} = state
        state={a,succesor,c,d,e}
        {:reply,b, state}
    end

    @doc """
    Updates the number of Requests the node with PID <pid> can make.
    """
    def updateRequestState(pid, request) do
        GenServer.call(pid, {:UpdateRequestState,request})
    end

    @doc false
    defp handle_call({:UpdateRequestState,request}, _from ,state) do
        {a,b,c,d,e} = state
        state={a,b,c,d,request}
        {:reply,e, state}
    end

    @doc """
    Updates the keys stored in the node with PID <pid>
    """
    def updateKeyState(pid, key) do
        GenServer.call(pid, {:UpdateKeyState, key})
    end

    @doc false
    defp handle_call({:UpdateKeyState, key}, _from ,state) do
        {a,b,c,d,e} = state
        state={a,b,c, d ++ [key],e}
        # IO.inspect state
        {:reply,d ++ [key], state}
    end

    # @doc """
    # Updates the Finger Table of the node with PID <pid>
    # """
    @doc false
    def updateFingersState(pid, finger) do
        GenServer.call(pid, {:UpdateFingersState,finger})
    end

    @doc false
    def handle_call({:UpdateFingersState,finger}, _from ,state) do
        {a,b,c,d,e} = state
        state={a,b,c ++ [finger],d,e}
        # IO.inspect finger
        {:reply,c ++ [finger], state}
    end

    @doc """
    Updates the PID and HashedPID of the node with PID <pid>
    """
    def updatePIDState(pid, hashPid) do
        GenServer.call(pid, {:UpdatePIDState,hashPid})
    end

    @doc false
    defp handle_call({:UpdatePIDState,hashPid}, _from ,state) do
        {a,b,c,d,e} = state
        state={hashPid,b,c,d,e}
        {:reply,a, state}
    end

    @doc """
    get the state of the current Node
    """
    def getState(pid) do
        GenServer.call(pid,{:GetState})
    end
    
    @doc false
    defp handle_call({:GetState}, _from ,state) do
        {a,b,c,d}=state
        # IO.inspect("b #{b}")
        {:reply,state, state}
    end


    def waitIndefinitely() do
        waitIndefinitely()
    end

    # // ask node n to find the successor of id
    # n.find successor(id)
    #   if (id ∈ (n,successor])
    #       return successor;
    #   else
    #       n' = closest preceding node(id);
    #       return n'.find successor(id);
    # 
    # // search the local table for the highest predecessor of id
    # n.closest preceding node(id)
    #   for i = m downto 1
    #   if (finger[i] ∈ (n,id))
    #       return finger[i];
    #   return n;
end

Chords1.main()
"""
c("../sem_1/DOS/ChordAlgorithm/chords/lib/generateRandomStrings.ex", ".")
c("../sem_1/DOS/ChordAlgorithm/chords/lib/hash.ex", ".")
c("../sem_1/DOS/ChordAlgorithm/chords/lib/common.ex", ".")
c("../sem_1/DOS/ChordAlgorithm/chords/lib/chords.exs", ".")
"""