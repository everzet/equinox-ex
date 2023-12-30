defmodule ExampleApp.CustomValidators do
  import Ecto.Changeset

  @uuid_regex ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$/i
  def validate_uuid(changeset, field, opts \\ []) do
    changeset
    |> validate_format(field, @uuid_regex, opts)
    |> update_change(field, &String.downcase/1)
  end

  @email_regex ~r/^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$/
  def validate_email(changeset, field, opts \\ []) do
    validate_format(changeset, field, @email_regex, opts)
  end
end
