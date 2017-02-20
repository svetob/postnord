defmodule Postnord.Doctest do
  use ExUnit.Case, async: true

  @moduledoc """
  Doctest for all modules that contain only doctests
  """

  doctest Postnord.IdGen
end
