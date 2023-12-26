defmodule ExampleApp.Validator do
  import Ecto.Changeset

  def validate(params, changeset_fun) do
    params
    |> changeset_fun.()
    |> apply_action(:validate)
  end

  def validate_uuid(changeset, field, opts \\ []) do
    validate_format(
      changeset,
      field,
      ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      opts
    )
  end

  def validate_email(changeset, field, opts \\ []) do
    validate_format(
      changeset,
      field,
      ~r/^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$/,
      opts
    )
  end
end
