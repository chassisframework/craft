defmodule Craft.GBTree do
  @empty_gb_tree :gb_trees.empty()

  def take_lte(tree, value, results \\ [])

  def take_lte(@empty_gb_tree, _value, results), do: {@empty_gb_tree, results}

  def take_lte(tree, value, results) do
    case :gb_trees.take_smallest(tree) do
      {this_value, result, new_tree} when this_value <= value ->
        take_lte(new_tree, value, [result | results])

      _ ->
        {tree, results}
    end
  end
end
