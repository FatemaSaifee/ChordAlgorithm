import GenerateRandomStrings
import Hash

defmodule Chords do
    def main do
        if (Enum.count(System.argv())!=2) do
            IO.puts" Illegal Arguments Provided"
            System.halt(1)
        else
            [numNodes, numRequests] = System.argv();
            {numNodes, _} = Integer.parse(numNodes);
            {numRequests, _} = Integer.parse(numRequests);
            if numNodes > 115792089237316195423570985008687907853269984665640564039457584007913129639936 do
                IO.puts("Number  of nodes should be less that 2^256")
            else 
                if numNodes < 0 do
                    IO.puts("Number of nodes should be positive")
                end
            
                pidHashMap = createNodes(numNodes, numRequests)
                pidHashMap = List.keysort(pidHashMap, 1)
                allKeys = createKeys(numNodes)
                buildRing(pidHashMap)
                assignKeysToNodes(allKeys, pidHashMap)
                createFingerTable(pidHashMap, numNodes)
            end
        end
    end

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

    def createKeys(numNodes) do
        allKeys = Enum.map((1..2*numNodes), fn(x) ->
            randomizer(5)
        end)
        allKeys
    end

    def buildRing(pidHashMap) do
        Enum.map(0..length(pidHashMap)-1, fn(x) ->
            pid = elem(Enum.fetch!(pidHashMap, x), 0)
            if x == length(pidHashMap)-1 do
                hashSuccesor = elem(Enum.fetch!(pidHashMap, 0), 1)
                updateSuccesorState(pid,hashSuccesor)
            else
                hashSuccesor = elem(Enum.fetch!(pidHashMap, x+1), 1)
                updateSuccesorState(pid,hashSuccesor)
            end
        end)

    end

    def createFingerTable(pidHashMap, numNodes) do
        m = round :math.floor(:math.log2(numNodes))
        Enum.map(0..length(pidHashMap)-1, fn(n) ->
            currentnode = Enum.fetch!(pidHashMap, n)
            # IO.inspect currentnode
            intHashNodeId = elem(Integer.parse(elem(currentnode, 1), 16), 0)
            Enum.each(0..m-1, fn(i) -> 
                powerOfTwo = Kernel.trunc(:math.pow(2,i))
                key = Integer.to_string(intHashNodeId+ powerOfTwo, 16)
                # IO.puts key
                if elem(Enum.fetch!(pidHashMap, length(pidHashMap)-1), 0) < key do
                    successor = Enum.fetch!(pidHashMap, 0)
                    map = %{"key": key, "value": successor }
                    updateFingersState(elem(currentnode,0), map)
                else
                    IO.puts "greater condition"
                    result = Enum.map(0..length(pidHashMap)-1, fn(x) -> 
                        element = Enum.fetch!(pidHashMap, x)
                        if key <elem(element,1) do
                            x
                        end
                    end)

                    # IO.inspect result
                    successor = Enum.fetch!(pidHashMap, Enum.fetch!(Enum.reject(result, &is_nil/1),0))
                    map = %{"key": key, "value": successor }
                    updateFingersState(elem(currentnode,0), map)
                end
            end)
        end)
       
    end

    def assignKeysToNodes(allKeys, pidHashMap) do
        # Assign  a key to the node when hash of key is just less that hash of PID of the node
        # IO.inspect pidHashMap
       
        Enum.each(allKeys, fn(key) -> 
            hashedKey = generateHash(key)
            # IO.inspect  String.slice(hashedKey, 0..3)
            count = 0
            allHashedNodes = []
            
            result = Enum.map(0..length(pidHashMap)-1, fn(x) -> 
                element = Enum.fetch(pidHashMap, x)
                if hashedKey < elem(elem(element,1), 1) do
                    x
                end
            end)

            # IO.inspect result

            # successor = Enum.fetch!(pidHashMap, Enum.fetch!(Enum.reject(result, &is_nil/1),0))
            
        end)
        
    end

    def init(:ok) do
        {:ok, {'', '', [], [], 0}}
    end

    def start_node() do
        {:ok,pid}=GenServer.start_link(__MODULE__, :ok,[])
        pid
    end

    def updateSuccesorState(pid,succesor) do
        GenServer.call(pid, {:UpdateSuccesorState,succesor})
    end

    def handle_call({:UpdateSuccesorState,succesor}, _from ,state) do
        {a,b,c,d,e} = state
        state={a,succesor,c,d,e}
        {:reply,b, state}
    end

    def updateRequestState(pid,request) do
        GenServer.call(pid, {:UpdateRequestState,request})
    end

    def handle_call({:UpdateRequestState,request}, _from ,state) do
        {a,b,c,d,e} = state
        state={a,b,c,d,request}
        {:reply,e, state}
    end

    def updateKeyState(pid, key) do
         GenServer.call(pid, {:UpdateKeyState,key})
    end

    def handle_call({:UpdateKeyState,key}, _from ,state) do
        {a,b,c,d,e} = state
        state={a,b,c, d ++ [key],e}
        # IO.inspect state
        {:reply,d ++ [key], state}
    end

    def updateFingersState(pid, finger) do
         GenServer.call(pid, {:UpdateFingersState,finger})
    end

    def handle_call({:UpdateFingersState,finger}, _from ,state) do
        {a,b,c,d,e} = state
        state={a,b,c ++ [finger],d,e}
        # IO.inspect finger.value
        {:reply,c ++ [finger], state}
    end

    # def updateState(pid,state) do
    #     GenServer.call(pid, {:UpdateState,state})
    # end

    # def handle_call({:UpdateState,newState}, _from ,state) do
    #     {:reply, newState}
    # end

    def updatePIDState(pid,hashPid) do
        GenServer.call(pid, {:UpdatePIDState,hashPid})
    end

    def handle_call({:UpdatePIDState,hashPid}, _from ,state) do
        {a,b,c,d,e} = state
        state={hashPid,b,c,d,e}
        {:reply,a, state}
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

Chords.main()