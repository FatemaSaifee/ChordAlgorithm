import GenerateRandomStrings

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
            end
        end
    end

    def createNodes(numNodes, numRequests) do
        # allHashedNodes = []
        allHashedNodes = Enum.map((1..numNodes), fn(x) ->
            pid=start_node()
            pidStr  = Kernel.inspect(pid)
            hashPid = :crypto.hash(:sha256, pidStr) |> Base.encode16
            # allHashedNodes = allHashedNodes ++ [hashPid]
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

    def assignKeysToNodes(allKeys, pidHashMap) do
        # Assign  a key to the node when hash of key is just less that hash of PID of the node
        IO.inspect allKeys
        IO.inspect pidHashMap
       
        Enum.each(allKeys, fn(key) -> 
            hashedKey = :crypto.hash(:sha256, key) |> Base.encode16
            count = 0
             Enum.each(pidHashMap, fn(x) -> 
                case elem(pidHashMap[count], 1) < hashedKey -> 
                # if hashedKey > elem(x, 1) do
                #     count = count + 1
                #     # if count == 1 do
                #     fetch_something()
                #     updateKeyState(elem(x, 0), key)
                #     break
                #     # end
                # end
                # elem(pidHashMap[count], 1) < hashedKey
                case count do
                    x when x > 1 ->
                        # count = count + 1
                    x when x < 1 ->
                        if hashedKey > elem(x, 1) do
                            count = count + 1
                            updateKeyState(elem(x, 0), key)
                        end
                end  
             end)

            
        end)


        
    end

    def loop(p, i) do
        case i in p do
            true -> loop(p, i + 0.01)
            false -> i
        end
    end

    def init(:ok) do
        # {:ok, {hash(self), succesor, fingers[], keys[], numRequest}}
        {:ok, {'', '', [], [], 0}}
    end

    def start_node() do
        {:ok,pid}=GenServer.start_link(__MODULE__, :ok,[])
        pid
    end

    # update the Count for a process for Push Sum
    def updateSuccesorState(pid,succesor) do
        GenServer.call(pid, {:UpdateSuccesorState,succesor})
    end

    def handle_call({:UpdateSuccesorState,succesor}, _from ,state) do
        {a,b,c,d,e} = state
        state={a,succesor,c,d,e}
        IO.inspect state
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
        IO.inspect state
        {:reply,d ++ [key], state}
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