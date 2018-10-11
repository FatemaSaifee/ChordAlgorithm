defmodule Chords do
    def main do
        if (Enum.count(args)!=2) do
            IO.puts" Illegal Arguments Provided"
            System.halt(1)
        else
            [numNodes, numRequests] = System.argv();
            {numRequests, _} = Integer.parse(numNodes);
            
        end
    end
end