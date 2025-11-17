package internal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

func TestComputeSchemaDriftDetectsAddedAndDroppedColumns(t *testing.T) {
	dst := "public.demo_users_dst"
	tableMappings := []*protos.TableMapping{{
		SourceTableIdentifier:      "public.demo_users",
		DestinationTableIdentifier: dst,
	}}

	current := map[string]*protos.TableSchema{
		dst: {
			TableIdentifier: dst,
			System:          protos.TypeSystem_PG,
			NullableEnabled: true,
			Columns: []*protos.FieldDescription{
				{Name: "id", Type: "int8"},
				{Name: "name", Type: "text"},
			},
		},
	}

	latest := map[string]*protos.TableSchema{
		dst: {
			TableIdentifier: dst,
			System:          protos.TypeSystem_PG,
			NullableEnabled: true,
			Columns: []*protos.FieldDescription{
				{Name: "id", Type: "int8"},
				{Name: "email", Type: "text"},
			},
		},
	}

	deltas := ComputeSchemaDrift(tableMappings, current, latest)
	require.Len(t, deltas, 1)
	delta := deltas[0]
	require.Equal(t, "public.demo_users", delta.SrcTableName)
	require.Equal(t, dst, delta.DstTableName)
	require.Len(t, delta.AddedColumns, 1)
	require.Equal(t, "email", delta.AddedColumns[0].Name)
	require.Len(t, delta.DroppedColumns, 1)
	require.Equal(t, "name", delta.DroppedColumns[0].Name)
}

func TestComputeSchemaDriftDetectsTypeChange(t *testing.T) {
	dst := "public.demo_users_dst"
	tableMappings := []*protos.TableMapping{{
		SourceTableIdentifier:      "public.demo_users",
		DestinationTableIdentifier: dst,
	}}

	current := map[string]*protos.TableSchema{
		dst: {
			TableIdentifier: dst,
			System:          protos.TypeSystem_PG,
			Columns: []*protos.FieldDescription{
				{Name: "age", Type: "int4"},
			},
		},
	}

	latest := map[string]*protos.TableSchema{
		dst: {
			TableIdentifier: dst,
			System:          protos.TypeSystem_PG,
			Columns: []*protos.FieldDescription{
				{Name: "age", Type: "int8"},
			},
		},
	}

	deltas := ComputeSchemaDrift(tableMappings, current, latest)
	require.Len(t, deltas, 1)
	delta := deltas[0]
	require.Len(t, delta.AddedColumns, 1)
	require.Equal(t, "age", delta.AddedColumns[0].Name)
	require.Len(t, delta.DroppedColumns, 1)
	require.Equal(t, "age", delta.DroppedColumns[0].Name)
}
