import GenerateRandomStrings
import Hash
# import Common

defmodule Chords do
    @moduledoc """
    Chord is a protocol and algorithm for a peer-to-peer distributed hash table. 
    A distributed hash table stores key-value pairs by assigning keys to different computers (known as "nodes"); a node will store the values for all the keys for which it is responsible. 
    Chord specifies how keys are assigned to nodes, and how a node can discover the value for a given key by first locating the node responsible for that key.
    """

    @doc """
    Generate random string based on the given legth. It is also possible to generate certain type of randomise string using the options below:
    * :numNodes -  the number of peers to be created in the peer to peer system 
    * :numRequests - the number of requests each peer has to make.
    When all peers performed that many requests, the program can exit.
    Each peer shouldsend a request/second.
    ## Example
        iex> Chords.main(1000, 20) //"The average number of hops (node connections) that have to be traversed to deliever a message is 256."
    """
    def main do
        if (Enum.count(System.argv())!=2) do
            IO.puts" Illegal Arguments Provided"
            System.halt(1)
        # if false do
        #     IO.puts" Illegal Arguments Provided"
        else
            # [numNodes, numRequests] = ["2", "1"]
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
                        table = :ets.new(:table, [:named_table,:public])
                        :ets.insert(table, {"globalCount",0})
                        :ets.insert(table, {"globalHopsCount",0})
                        # IO.inspect {numNodes, numRequests}
                        allNodesMap = createNodes(numNodes, numRequests)
                        pidHashMap = List.keysort(allNodesMap, 1)
                        allKeys = createKeys(numNodes)
                        buildRing(pidHashMap)
                        assignKeysToNodes(allKeys, pidHashMap)
                        createFingerTables(pidHashMap, numNodes)
                        # :timer.sleep 10000
                        Enum.each(pidHashMap, fn(x) -> 
                            IO.inspect getState(elem(x, 0))
                        end)
                        startTransmit(pidHashMap, allKeys, numRequests)
                        # :timer.sleep 10000
                        # count = :ets.update_counter(:table, "globalCount", {2,1})
                        # IO.puts count
                        waitIndefinitely()
                    end
                end
            end
        end
    end

    @doc """
    Creates <numNodes> Nodes, i.e. Processes. We collect all the PIDs of these processes and hash them. Finally we return a list of PIDs and their respective hashes. Arguments are as follows:
    * :numNodes -  the number of peers to be created in the peer to peer system 
    * :numRequests - the number of requests each peer has to make.
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
            totalCount = numRequests*numNodes
            updateRequestState(pid, numRequests, totalCount)
            {pid, hashPid}
        end)
        allHashedNodes
    end

    @doc """
    create (2 * <numNodes>) random keys using GenerateRandomStrings module. Arguments are as follows:
    * numNodes -  the number of peers to be created in the peer to peer system 
    ## Example
        iex> Chords.createKeys(2) 
        //Output
        ["4Le7C", "WKW2g", "TteAa", "kXi4L"]
    """
    def createKeys(numNodes) do
        allKeys = Enum.map((1..4*numNodes), fn(x) ->
            randomizer(5)
        end)
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
    * pidHashMap -  {PID, hashedPID} list Sorted on hashedPIDs
    """
    def buildRing(pidHashMap) do
        Enum.map(0..length(pidHashMap)-1, fn(x) ->
            pid = elem(Enum.fetch!(pidHashMap, x), 0)
            if x == length(pidHashMap)-1 do
                hashSuccesor = elem(Enum.fetch!(pidHashMap, 0), 0)
                updateSuccesorState(pid,hashSuccesor)
            else
                succesor = elem(Enum.fetch!(pidHashMap, x+1), 0)
                updateSuccesorState(pid,succesor)
            end
        end)

    end

    @doc """
    Returns computed key for finger k
    * k - from 0 to (m - 1)
    """
    def calcfinger(currentnode, k) do
        intHashNodeId = elem(Integer.parse(elem(currentnode, 1), 16), 0)
        floorK = round :math.floor(255/(k+1))
        powerOfTwo = Kernel.trunc(:math.pow(2,floorK))
        nextFinger = Integer.to_string(intHashNodeId + powerOfTwo, 16)
        nextFinger
    end

    @doc """
    To avoid the linear search above, Chord implements a faster search method by requiring each node to keep a finger table containing up to m entries, recall that m is the number of bits in the hash key. 
    The i^{th} entry of node n will contain successor((n+2^{i-1}),mod,2^m). 
    The first entry of finger table is actually the node's immediate successor (and therefore an extra successor field is not needed). 
    Every time a node wants to look up a key k, it will pass the query to the closest successor or predecessor (depending on the finger table) of  k in its finger table (the "largest" one on the circle whose ID is smaller than  k), until a node finds out the key is stored in its immediate successor.
    With such a finger table, the number of nodes that must be contacted to find a successor in an N-node network is  O(log N). 
    """
    def createFingerTables(pidHashMap, numNodes) do
        m = round :math.floor(:math.log2(numNodes))
        Enum.map(0..length(pidHashMap)-1, fn(n) ->
            currentnode = Enum.fetch!(pidHashMap, n)
            Enum.each(0..(m-1), fn(i) -> 
                
                nextFinger = calcfinger(currentnode, i)
               
                if elem(Enum.fetch!(pidHashMap, length(pidHashMap)-1), 1) < nextFinger do
                    # IO.puts "Not greater condition"
                    successor = Enum.fetch!(pidHashMap, 0)
                    map = %{"nextFinger": nextFinger, "successor": successor }
                    updateFingersState(elem(currentnode,0), map)
                else
                    # IO.puts "greater condition"
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
    Start the lookup task for each node in the Identity circle.
    Each node must initiate <numRequests> lookup task with a random generated key from allKeys list
    """
    def startTransmit(pidHashMap, allKeys, numRequests) do
        #  IO.inspect pidHashMap
        #  IO.inspect allKeys
        totalCount = length(pidHashMap) * numRequests 
        Enum.map(0..numRequests-1, fn(i) -> 
            Enum.each(pidHashMap, fn(x) ->
                Task.start(Chords,:lookup,[x, allKeys, totalCount, x])
                # :timer.sleep 200
            end)
        end)
        
    end
    
    @doc false
    def lookup(currentNode , keyList, totalCount, startNode) do
        key = Enum.random(keyList)
        
        currentnodeId = elem(currentNode, 0)
        startNodeId = elem(startNode, 0)
        {currentnodeHashId, successor, fingers, keys, numRequests} = getState(currentnodeId)
        {_, _, _, _, startNumRequests} = getState(startNodeId)

        isPresent = Enum.member?(keys, key)
        
        if isPresent do
            # found
            # IO.puts "key found #{key}"
            updateRequestState(startNodeId, startNumRequests-1, totalCount)
            # :timer.sleep 2
        else
            # hops = :ets.update_counter(:table, "globalHopsCount", {2,1})
        
            Enum.map(0..length(fingers)-1, fn(x)->
                if Enum.fetch!(fingers, x)[:nextFinger] > generateHash(key) do
                    newSuccessor = Enum.fetch!(fingers, x)[:successor]
                    successorId = elem(newSuccessor, 0)
                    {currentnodeHashIdNew, successorNew, fingersNew, keysNew, numRequestsNew} = getState(successorId)
                    if length(keysNew) > 0 do
                        isPresent = Enum.member?(keysNew, key)

                        if isPresent do
                            # found
                            # IO.puts "key found #{key}"
                            updateRequestState(startNodeId, startNumRequests-1, totalCount)
                            # :timer.sleep 2
                        else
                            # not found
                            # @param [key] - list of one key to be searched so that during recursion random gives us the same key
                            if x == 0 do
                                prevFinger = Enum.fetch!(fingers, length(fingers)-1)[:successor]
                                lookup(prevFinger, [key], totalCount, startNode)
                            else
                                prevFinger = Enum.fetch!(fingers, x-1)[:successor]
                                lookup(prevFinger, [key], totalCount, startNode)
                            end
                            hops = :ets.update_counter(:table, "globalHopsCount", {2,1})
                            IO.puts "hops = #{hops}"
                        end
                    end
                    
                end

            end)
        end
        # :timer.sleep(2)

    end

    

    @doc """
    Assign  a key to the node when hash of key is just less that hash of PID of the node.
    Key k is assigned to the first node whose key is ≥ k (called the successor node of key k)
    Arguments are as follows:
    * allKeys -  list of all keys to be stored in the peer-to-peer system
    * pidHashMap -  list of {PID, hashedPID} Sorted on hashedPIDs
    """
    def assignKeysToNodes(allKeys, pidHashMap) do
        Enum.each(allKeys, fn(key) -> 
            hashedKey = generateHash(key)
            # IO.puts String.slice(hashedKey, 0..3)
            # IO.puts String.slice(elem(Enum.fetch!(pidHashMap, length(pidHashMap)-1), 1), 0..3)
            if hashedKey > elem(Enum.fetch!(pidHashMap, length(pidHashMap)-1), 1) do
                succesor =  Enum.fetch!(pidHashMap, 0)
                updateKeyState(elem(succesor, 0), key)
                
            else
                count = 0
                allHashedNodes = []
                
                result = Enum.map(0..length(pidHashMap)-1, fn(x) -> 
                    element = Enum.fetch!(pidHashMap, x)
                    if hashedKey < elem(element,1) do
                        x
                    end
                end)
                successor = Enum.fetch!(pidHashMap, Enum.fetch!(Enum.reject(result, &is_nil/1),0))
                updateKeyState(elem(successor, 0), key)
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
    def handle_call({:UpdateSuccesorState,succesor}, _from, state) do
        {a,b,c,d,e} = state
        state={a,succesor,c,d,e}
        {:reply,b, state}
    end

    @doc """
    Updates the number of Requests the node with PID <pid> can make.
    """
    def updateRequestState(pid, request, totalCount) do
        # IO.inspect pid
        GenServer.call(pid, {:UpdateRequestState, request, totalCount})
    end

    @doc false
    def handle_call({:UpdateRequestState, request, totalCount}, _from, state) do
        {a,b,c,d,e} = state
        state={a,b,c,d,request}
        # IO.puts "UpdateRequestState #{request}"
        if request == 0 do 
            Process.exit(self(), :kill)
        end
        if request <= 0 do
            count = :ets.update_counter(:table, "globalCount", {2,1})
            IO.puts "Found key number #{count}"
            result = round (:ets.update_counter(:table, "globalHopsCount", {2,0}))/totalCount
            
            # IO.inspect b 
            if count == totalCount  do
                IO.puts "The average number of hops (node connections) that have to be traversed to deliever a message is #{result}. (#{totalCount})"
                System.halt(1)
            end
        end
        {:reply,e, state}
    end

    @doc """
    Updates the keys stored in the node with PID <pid>
    """
    def updateKeyState(pid, key) do
         GenServer.call(pid, {:UpdateKeyState,key})
    end

    @doc false
    def handle_call({:UpdateKeyState,key}, _from, state) do
        {a,b,c,d,e} = state
        state={a,b,c, d ++ [key],e}
        # IO.inspect {a, d ++ [key]}
        {:reply,d ++ [key], state}
    end

    @doc """
    Updates the Finger Table of the node with PID <pid>
    """
    def updateFingersState(pid, finger) do
         GenServer.call(pid, {:UpdateFingersState,finger})
    end

    @doc false
    def handle_call({:UpdateFingersState,finger}, _from, state) do
        {a,b,c,d,e} = state
        state={a,b,c ++ [finger],d,e}
        # IO.puts "FFFFFFFF"
        # IO.inspect state
        {:reply,c ++ [finger], state}
    end

    @doc """
    Updates the PID and HashedPID of the node with PID <pid>
    """
    def updatePIDState(pid, hashPid) do
        GenServer.call(pid, {:UpdatePIDState,hashPid})
    end

    @doc false
    def handle_call({:UpdatePIDState,hashPid}, _from, state) do
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
    def handle_call({:GetState}, _from, state) do
        {a,b,c,d,e}=state
        # IO.inspect("b #{b}")
        {:reply,state, state}
    end

    @doc """
    Makes the system wait till the program is terminated
    """
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

    

    # @doc """
    # Returns the node responsible for finger k
    # * m: Id length of the ring. (m = Key.idlength)
    #     Ring is constituted of 2^m nodes maximum
    # """
    # def lookupfinger(k, currentnode, useOnlySucc \\ false) do
    #     lookup(calcfinger(currentnode, k), currentnode, useOnlySucc)
    # end

    # @doc """
    # UPdate finger table for all ring in a clockwise around successor fashion
    # finger is an array of dict {resp, key}
    #     `resp` is the Node responsible for `key`
    # * newnode: new node which imply this update
    # * firstnode: node which launch the update
    # """
    # def updatefinger(currentnode, newnode, firstnode, numNodes) do
    #     m = round :math.floor(:math.log2(numNodes))
    #     Enum.each(0..m-1, fn(i) -> 
        
    #     # for i in range(0, self.uid.idlength) 
    #         fingerkey = calcfinger(i)
    #         resp = lookup(fingerkey, useOnlySucc=True)
    #         # map = %{"resp": resp, "key": fingerkey}
    #         map = %{"nextFinger": nextFinger, "successor": successor }
    #         updateFingersState(elem(currentnode,0), map)
    #     end)
    #     if elem(firstnode, 1) != elwm(successor, 1) do
    #         updatefinger(successor, newnode, firstnode)
    #     end
    # end
end

Chords.main()