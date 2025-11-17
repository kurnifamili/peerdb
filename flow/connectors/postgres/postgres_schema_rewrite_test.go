package connpostgres

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/e2eshared"
)

type PostgresSchemaRewriteTestSuite struct {
	t *testing.T
}

func SetupSchemaRewriteSuite(t *testing.T) PostgresSchemaRewriteTestSuite {
	t.Helper()
	return PostgresSchemaRewriteTestSuite{t: t}
}

func (suite PostgresSchemaRewriteTestSuite) Teardown(context.Context) {}

func (suite PostgresSchemaRewriteTestSuite) TestRewriteTableIdentifierInSQL() {
	suite.t.Helper()
	src := &utils.SchemaTable{Schema: "public", Table: "demo_users"}
	dst := &utils.SchemaTable{Schema: "replica", Table: "demo_users_dst"}

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "unquoted schema and table",
			input:    "CREATE INDEX idx_age_desc ON public.demo_users USING btree (age DESC)",
			expected: "CREATE INDEX idx_age_desc ON replica.demo_users_dst USING btree (age DESC)",
		},
		{
			name:     "fully quoted identifiers",
			input:    "CREATE TRIGGER user_change_log AFTER INSERT ON \"public\".\"demo_users\" EXECUTE FUNCTION log_user_changes()",
			expected: "CREATE TRIGGER user_change_log AFTER INSERT ON \"replica\".\"demo_users_dst\" EXECUTE FUNCTION log_user_changes()",
		},
		{
			name:     "mixed quoting",
			input:    "CREATE INDEX idx_partial ON \"public\".demo_users (email)",
			expected: "CREATE INDEX idx_partial ON \"replica\".demo_users_dst (email)",
		},
	}

	for _, tc := range testCases {
		suite.t.Run(tc.name, func(t *testing.T) {
			actual := rewriteTableIdentifierInSQL(tc.input, src, dst)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func (suite PostgresSchemaRewriteTestSuite) TestRewriteIndexNameInSQL() {
	suite.t.Helper()
	src := &utils.SchemaTable{Schema: "public", Table: "demo_users"}
	dst := &utils.SchemaTable{Schema: "public", Table: "demo_users_dst"}
	original := "CREATE INDEX idx_email ON public.demo_users USING btree (email)"
	input := rewriteTableIdentifierInSQL(original, src, dst)
	dstIndexName := "idx_email__public_demo_users_dst"
	actual := rewriteIndexNameInSQL(input, src, dst, "idx_email", dstIndexName)
	require.Contains(suite.t, actual, dstIndexName)
	require.Contains(suite.t, actual, "ON public.demo_users_dst")
}

func (suite PostgresSchemaRewriteTestSuite) TestBuildDestinationIndexNameLength() {
	suite.t.Helper()
	dst := &utils.SchemaTable{Schema: "public", Table: strings.Repeat("a", 32)}
	sourceName := strings.Repeat("b", 40)
	result := buildDestinationIndexName(sourceName, dst)
	require.LessOrEqual(suite.t, len(result), postgresIdentifierMaxLength)
	require.NotEmpty(suite.t, result)
}

func TestPostgresSchemaRewriteTestSuite(t *testing.T) {
	e2eshared.RunSuite(t, SetupSchemaRewriteSuite)
}
