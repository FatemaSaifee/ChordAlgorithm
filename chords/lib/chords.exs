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
            
                createNodes(numNodes, numRequests)
                createKeys(numNodes)
                buildRing(numNodes)
            end
        end
    end

    def createNodes(numNodes, numRequests) do
        allNodes = Enum.map((1..numNodes), fn(x) ->
            pid=start_node(numRequests)
            hashPid = :crypto.hash(:sha256, pid) |> Base.encode16
            updatePIDState(pid, hashPid)
            updateRequestState(pid, numRequests)
            pid
        end)
    end

    def createKeys(numNodes) do
        allNodes = Enum.map((1..numNodes), fn(x) ->
            pid=start_node(numRequests)
            hashPid = :crypto.hash(:sha256, pid) |> Base.encode16
            updatePIDState(pid, hashPid)
            updateRequestState(pid, numRequests)
            pid
        end)
    end

    def buildRing(numNodes) do
        
    end

    def init(:ok) do
        # {:ok, {hash(self), succesor, fingers[], keys[], numRequest}}
        IO.puts numRequests
        {:ok, {'', '', [], [], 0}}
    end

    def start_node(numRequests) do
        {:ok,pid}=GenServer.start_link(__MODULE__, :ok,[numRequests])
        pid
    end

    # update the Count for a process for Push Sum
    def updateSuccesor(pid,succesor) do
        GenServer.call(pid, {:UpdatePSCount,succesor})
    end

    def handle_call({:UpdateSuccesor,succesor}, _from ,state) do
        {a,b,c,d} = state
        state={a,succesor,c,d}
        {:reply,b, state}
    end

    def updateRequest(pid,request) do
        GenServer.call(pid, {:UpdateRequest,request})
    end

    def handle_call({:UpdateRequest,request}, _from ,state) do
        {a,b,c,d} = state
        state={a,b,c,d,request}
        {:reply,e, state}
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