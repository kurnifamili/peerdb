package connpostgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type PostgresTriggerTestSuite struct {
	t         *testing.T
	connector *PostgresConnector
	schema    string
}

func SetupTriggerSuite(t *testing.T) PostgresTriggerTestSuite {
	t.Helper()

	connector, err := NewPostgresConnector(t.Context(), nil, internal.GetCatalogPostgresConfigFromEnv(t.Context()))
	require.NoError(t, err)

	setupTx, err := connector.conn.Begin(t.Context())
	require.NoError(t, err)
	defer func() {
		err := setupTx.Rollback(t.Context())
		if err != pgx.ErrTxClosed {
			require.NoError(t, err)
		}
	}()
	schema := "pgtrigger_" + strings.ToLower(shared.RandomString(8))
	_, err = setupTx.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	require.NoError(t, err)
	_, err = setupTx.Exec(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	require.NoError(t, setupTx.Commit(t.Context()))

	return PostgresTriggerTestSuite{
		t:         t,
		connector: connector,
		schema:    schema,
	}
}

func (suite PostgresTriggerTestSuite) teardown() {
	suite.t.Helper()
	_, err := suite.connector.conn.Exec(suite.t.Context(),
		fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", suite.schema))
	require.NoError(suite.t, err)
	suite.connector.Close()
}

func (suite PostgresTriggerTestSuite) TestGetAllTriggers() {
	tableName := "test_triggers"
	schemaQualifiedTable := fmt.Sprintf("%s.%s", suite.schema, tableName)

	// Create a table with multiple triggers
	_, err := suite.connector.conn.Exec(suite.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			email TEXT,
			updated_at TIMESTAMP
		);

		CREATE OR REPLACE FUNCTION update_timestamp()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = NOW();
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		CREATE OR REPLACE FUNCTION log_email_change()
		RETURNS TRIGGER AS $$
		BEGIN
			RAISE NOTICE 'Email changed from %% to %%', OLD.email, NEW.email;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER trigger_update_timestamp
			BEFORE UPDATE ON %s
			FOR EACH ROW
			EXECUTE FUNCTION update_timestamp();

		CREATE TRIGGER trigger_log_email
			AFTER UPDATE OF email ON %s
			FOR EACH ROW
			EXECUTE FUNCTION log_email_change();
	`, schemaQualifiedTable, schemaQualifiedTable, schemaQualifiedTable))
	require.NoError(suite.t, err)

	// Get triggers
	triggers, err := suite.connector.GetTriggers(suite.t.Context(),
		&utils.SchemaTable{Schema: suite.schema, Table: tableName})
	require.NoError(suite.t, err)
	require.Len(suite.t, triggers, 2, "expected 2 triggers")

	// Verify trigger names
	triggerNames := make([]string, len(triggers))
	for i, trig := range triggers {
		triggerNames[i] = trig.TriggerName
	}
	require.Contains(suite.t, triggerNames, "trigger_update_timestamp")
	require.Contains(suite.t, triggerNames, "trigger_log_email")

	// Verify trigger definitions are not empty
	for _, trig := range triggers {
		require.NotEmpty(suite.t, trig.TriggerDef, "trigger definition should not be empty")
		require.Contains(suite.t, trig.TriggerDef, "CREATE TRIGGER")
	}

	suite.teardown()
}

func (suite PostgresTriggerTestSuite) TestGetAllTriggersEmptyTable() {
	tableName := "test_no_triggers"
	schemaQualifiedTable := fmt.Sprintf("%s.%s", suite.schema, tableName)

	// Create a table without triggers
	_, err := suite.connector.conn.Exec(suite.t.Context(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			data TEXT
		);
	`, schemaQualifiedTable))
	require.NoError(suite.t, err)

	// Get triggers
	triggers, err := suite.connector.GetTriggers(suite.t.Context(),
		&utils.SchemaTable{Schema: suite.schema, Table: tableName})
	require.NoError(suite.t, err)
	require.Empty(suite.t, triggers, "expected no triggers")

	suite.teardown()
}

func (suite PostgresTriggerTestSuite) TestGetAllTriggersNonexistentTable() {
	tableName := "nonexistent_table"

	// Get triggers for nonexistent table
	triggers, err := suite.connector.GetTriggers(suite.t.Context(),
		&utils.SchemaTable{Schema: suite.schema, Table: tableName})
	require.NoError(suite.t, err)
	require.Empty(suite.t, triggers, "expected no triggers for nonexistent table")

	suite.teardown()
}

func TestPostgresTriggerTestSuite(t *testing.T) {
	t.Run("TestGetAllTriggers", func(t *testing.T) {
		suite := SetupTriggerSuite(t)
		suite.TestGetAllTriggers()
	})

	t.Run("TestGetAllTriggersEmptyTable", func(t *testing.T) {
		suite := SetupTriggerSuite(t)
		suite.TestGetAllTriggersEmptyTable()
	})

	t.Run("TestGetAllTriggersNonexistentTable", func(t *testing.T) {
		suite := SetupTriggerSuite(t)
		suite.TestGetAllTriggersNonexistentTable()
	})
}
