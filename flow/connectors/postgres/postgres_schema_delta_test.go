package connpostgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/e2eshared"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

type PostgresSchemaDeltaTestSuite struct {
	t         *testing.T
	connector *PostgresConnector
	schema    string
}

func SetupSuite(t *testing.T) PostgresSchemaDeltaTestSuite {
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
	schema := "pgdelta_" + strings.ToLower(shared.RandomString(8))
	_, err = setupTx.Exec(t.Context(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	require.NoError(t, err)
	_, err = setupTx.Exec(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	require.NoError(t, setupTx.Commit(t.Context()))

	return PostgresSchemaDeltaTestSuite{
		t:         t,
		connector: connector,
		schema:    schema,
	}
}

func (s PostgresSchemaDeltaTestSuite) TestSimpleAddColumn() {
	tableName := s.schema + ".simple_add_column"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: []*protos.FieldDescription{
			{
				Name:         "hi",
				Type:         string(types.QValueKindInt64),
				TypeModifier: -1,
				Nullable:     true,
			},
		},
	}}))

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		System:            protos.TypeSystem_Q,
		Columns: []*protos.FieldDescription{
			{
				Name:         "id",
				Type:         string(types.QValueKindInt32),
				TypeModifier: -1,
			},
			{
				Name:         "hi",
				Type:         string(types.QValueKindInt64),
				TypeModifier: -1,
				Nullable:     true,
			},
		},
	}, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestAddAllColumnTypes() {
	tableName := s.schema + ".add_drop_all_column_types"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		Columns:           AddAllColumnTypesFields,
		System:            protos.TypeSystem_Q,
	}
	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != "id" {
			addedColumns = append(addedColumns, column)
		}
	}

	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}}))

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestAddTrickyColumnNames() {
	tableName := s.schema + ".add_drop_tricky_column_names"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		Columns:           TrickyFields,
		System:            protos.TypeSystem_Q,
	}
	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != "id" {
			addedColumns = append(addedColumns, column)
		}
	}

	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}}))

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestAddDropWhitespaceColumnNames() {
	tableName := s.schema + ".add_drop_whitespace_column_names"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(\" \" INT PRIMARY KEY)", tableName))
	require.NoError(s.t, err)

	expectedTableSchema := &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{" "},
		Columns:           WhitespaceFields,
		System:            protos.TypeSystem_Q,
	}
	addedColumns := make([]*protos.FieldDescription, 0)
	for _, column := range expectedTableSchema.Columns {
		if column.Name != " " {
			addedColumns = append(addedColumns, column)
		}
	}

	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		AddedColumns: addedColumns,
	}}))

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, expectedTableSchema, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestSimpleDropColumn() {
	tableName := s.schema + ".simple_drop_column"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY, col1 TEXT, col2 INT)", tableName))
	require.NoError(s.t, err)

	// Insert some data to ensure DROP works with existing data
	_, err = s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, col1, col2) VALUES (1, 'test', 42)", tableName))
	require.NoError(s.t, err)

	// Drop col2
	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		DroppedColumns: []*protos.FieldDescription{
			{
				Name:         "col2",
				Type:         string(types.QValueKindInt32),
				TypeModifier: -1,
			},
		},
	}}))

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		System:            protos.TypeSystem_Q,
		Columns: []*protos.FieldDescription{
			{
				Name:         "id",
				Type:         string(types.QValueKindInt32),
				TypeModifier: -1,
			},
			{
				Name:         "col1",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
				Nullable:     true,
			},
		},
	}, output[tableName])

	// Verify the data is still there
	var id int
	var col1 string
	err = s.connector.conn.QueryRow(s.t.Context(),
		fmt.Sprintf("SELECT id, col1 FROM %s WHERE id = 1", tableName)).Scan(&id, &col1)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, id)
	require.Equal(s.t, "test", col1)
}

func (s PostgresSchemaDeltaTestSuite) TestDropMultipleColumns() {
	tableName := s.schema + ".drop_multiple_columns"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY, col1 TEXT, col2 INT, col3 BOOLEAN)", tableName))
	require.NoError(s.t, err)

	// Drop col2 and col3
	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		DroppedColumns: []*protos.FieldDescription{
			{
				Name:         "col2",
				Type:         string(types.QValueKindInt32),
				TypeModifier: -1,
			},
			{
				Name:         "col3",
				Type:         string(types.QValueKindBoolean),
				TypeModifier: -1,
			},
		},
	}}))

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		System:            protos.TypeSystem_Q,
		Columns: []*protos.FieldDescription{
			{
				Name:         "id",
				Type:         string(types.QValueKindInt32),
				TypeModifier: -1,
			},
			{
				Name:         "col1",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
				Nullable:     true,
			},
		},
	}, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestDropNonExistentColumn() {
	tableName := s.schema + ".drop_nonexistent_column"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY, col1 TEXT)", tableName))
	require.NoError(s.t, err)

	// Try to drop a column that doesn't exist - should not error due to IF EXISTS
	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		DroppedColumns: []*protos.FieldDescription{
			{
				Name:         "nonexistent",
				Type:         string(types.QValueKindInt32),
				TypeModifier: -1,
			},
		},
	}}))

	// Table should be unchanged
	output, err := s.connector.GetTableSchema(s.t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		System:            protos.TypeSystem_Q,
		Columns: []*protos.FieldDescription{
			{
				Name:         "id",
				Type:         string(types.QValueKindInt32),
				TypeModifier: -1,
			},
			{
				Name:         "col1",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
				Nullable:     true,
			},
		},
	}, output[tableName])
}

func (s PostgresSchemaDeltaTestSuite) TestDropAndAddSameColumn() {
	tableName := s.schema + ".drop_and_add_same_column"
	_, err := s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY, mycol TEXT)", tableName))
	require.NoError(s.t, err)

	// Insert some data with text type
	_, err = s.connector.conn.Exec(s.t.Context(),
		fmt.Sprintf("INSERT INTO %s(id, mycol) VALUES (1, 'text_value')", tableName))
	require.NoError(s.t, err)

	// Drop mycol (TEXT) and add mycol (INT) - simulating a column type change
	require.NoError(s.t, s.connector.ReplayTableSchemaDeltas(s.t.Context(), nil, "schema_delta_flow", nil, []*protos.TableSchemaDelta{{
		SrcTableName: tableName,
		DstTableName: tableName,
		DroppedColumns: []*protos.FieldDescription{
			{
				Name:         "mycol",
				Type:         string(types.QValueKindString),
				TypeModifier: -1,
			},
		},
		AddedColumns: []*protos.FieldDescription{
			{
				Name:         "mycol",
				Type:         string(types.QValueKindInt32),
				TypeModifier: -1,
				Nullable:     true,
			},
		},
	}}))

	output, err := s.connector.GetTableSchema(s.t.Context(), nil, shared.InternalVersion_Latest, protos.TypeSystem_Q,
		[]*protos.TableMapping{{SourceTableIdentifier: tableName}})
	require.NoError(s.t, err)
	require.Equal(s.t, &protos.TableSchema{
		TableIdentifier:   tableName,
		PrimaryKeyColumns: []string{"id"},
		System:            protos.TypeSystem_Q,
		Columns: []*protos.FieldDescription{
			{
				Name:         "id",
				Type:         string(types.QValueKindInt32),
				TypeModifier: -1,
			},
			{
				Name:         "mycol",
				Type:         string(types.QValueKindInt32),
				TypeModifier: -1,
				Nullable:     true,
			},
		},
	}, output[tableName])

	// Verify old data was deleted with the column, new column is NULL
	var id int
	var mycol *int
	err = s.connector.conn.QueryRow(s.t.Context(),
		fmt.Sprintf("SELECT id, mycol FROM %s WHERE id = 1", tableName)).Scan(&id, &mycol)
	require.NoError(s.t, err)
	require.Equal(s.t, 1, id)
	require.Nil(s.t, mycol) // Column was dropped and re-added, so value should be NULL
}

func TestPostgresSchemaDeltaTestSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupSuite)
}

func (s PostgresSchemaDeltaTestSuite) Teardown(ctx context.Context) {
	teardownTx, err := s.connector.conn.Begin(ctx)
	require.NoError(s.t, err)
	defer func() {
		err := teardownTx.Rollback(ctx)
		if err != pgx.ErrTxClosed {
			require.NoError(s.t, err)
		}
	}()
	_, err = teardownTx.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", s.schema))
	require.NoError(s.t, err)
	require.NoError(s.t, teardownTx.Commit(ctx))

	require.NoError(s.t, s.connector.ConnectionActive(ctx))
	require.NoError(s.t, s.connector.Close())
	require.Error(s.t, s.connector.ConnectionActive(ctx))
}
