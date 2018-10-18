defmodule Looper do
  def loop(p, i) do
    case i in p do
      true -> loop(p, i + 0.01)
      false -> i
    end
  end
end

p = [1.01, 1.02, 1.04]
i = 1.01

IO.inspect Looper.loop(p, i)