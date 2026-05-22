defmodule BedrockMinimalHostApp.Repo.Migrations.CreateLocalLedgerEntries do
  use Ecto.Migration

  def change do
    create table(:local_ledger_entries) do
      add :run_id, :string, null: false
      add :account_id, :string, null: false
      add :entry, :string, null: false

      timestamps()
    end

    create index(:local_ledger_entries, [:run_id])
  end
end
