package internal

import (
	"log/slog"
	"maps"
	"slices"

	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func AdditionalTablesHasOverlap(currentTableMappings []*protos.TableMapping,
	additionalTableMappings []*protos.TableMapping,
) bool {
	currentSrcTables := make([]string, 0, len(currentTableMappings))
	currentDstTables := make([]string, 0, len(currentTableMappings))
	additionalSrcTables := make([]string, 0, len(additionalTableMappings))
	additionalDstTables := make([]string, 0, len(additionalTableMappings))

	for _, currentTableMapping := range currentTableMappings {
		currentSrcTables = append(currentSrcTables, currentTableMapping.SourceTableIdentifier)
		currentDstTables = append(currentDstTables, currentTableMapping.DestinationTableIdentifier)
	}
	for _, additionalTableMapping := range additionalTableMappings {
		additionalSrcTables = append(additionalSrcTables, additionalTableMapping.SourceTableIdentifier)
		additionalDstTables = append(additionalDstTables, additionalTableMapping.DestinationTableIdentifier)
	}

	return shared.ArraysHaveOverlap(currentSrcTables, additionalSrcTables) ||
		shared.ArraysHaveOverlap(currentDstTables, additionalDstTables)
}

// given the output of GetTableSchema, processes it to be used by CDCFlow
// 1) changes the map key to be the destination table name instead of the source table name
// 2) performs column exclusion using protos.TableMapping as input.
func BuildProcessedSchemaMapping(
	tableMappings []*protos.TableMapping,
	tableNameSchemaMapping map[string]*protos.TableSchema,
	logger log.Logger,
) map[string]*protos.TableSchema {
	sortedSourceTables := slices.Sorted(maps.Keys(tableNameSchemaMapping))
	processedSchemaMapping := make(map[string]*protos.TableSchema, len(sortedSourceTables))

	for _, srcTableName := range sortedSourceTables {
		tableSchema := tableNameSchemaMapping[srcTableName]
		var dstTableName string
		for _, mapping := range tableMappings {
			if mapping.SourceTableIdentifier == srcTableName {
				dstTableName = mapping.DestinationTableIdentifier
				if len(mapping.Exclude) != 0 {
					columns := make([]*protos.FieldDescription, 0, len(tableSchema.Columns))
					pkeyColumns := make([]string, 0, len(tableSchema.PrimaryKeyColumns))
					for _, column := range tableSchema.Columns {
						if !slices.Contains(mapping.Exclude, column.Name) {
							columns = append(columns, column)
						}
						if slices.Contains(tableSchema.PrimaryKeyColumns, column.Name) &&
							!slices.Contains(mapping.Exclude, column.Name) {
							pkeyColumns = append(pkeyColumns, column.Name)
						}
					}
					tableSchema = &protos.TableSchema{
						TableIdentifier:       tableSchema.TableIdentifier,
						PrimaryKeyColumns:     pkeyColumns,
						IsReplicaIdentityFull: tableSchema.IsReplicaIdentityFull,
						NullableEnabled:       tableSchema.NullableEnabled,
						System:                tableSchema.System,
						Columns:               columns,
					}
				}
				break
			}
		}
		processedSchemaMapping[dstTableName] = tableSchema

		logger.Info("normalized table schema",
			slog.String("table", dstTableName),
			slog.Any("schema", tableSchema))
	}
	return processedSchemaMapping
}

func cloneFieldDescription(field *protos.FieldDescription) *protos.FieldDescription {
	if field == nil {
		return nil
	}
	return &protos.FieldDescription{
		Name:         field.Name,
		Type:         field.Type,
		TypeModifier: field.TypeModifier,
		Nullable:     field.Nullable,
	}
}

func columnsToMap(columns []*protos.FieldDescription) map[string]*protos.FieldDescription {
	result := make(map[string]*protos.FieldDescription, len(columns))
	for _, column := range columns {
		result[column.Name] = column
	}
	return result
}

func fieldDescriptionsEqual(a, b *protos.FieldDescription) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Name == b.Name && a.Type == b.Type && a.TypeModifier == b.TypeModifier && a.Nullable == b.Nullable
}

// ComputeSchemaDrift compares the persisted schema mapping with the latest schema fetched from source.
// It returns schema deltas that can be replayed at the destination to reconcile the difference.
func ComputeSchemaDrift(
	tableMappings []*protos.TableMapping,
	current map[string]*protos.TableSchema,
	latest map[string]*protos.TableSchema,
) []*protos.TableSchemaDelta {
	if len(tableMappings) == 0 || len(latest) == 0 {
		return nil
	}

	mappingByDst := make(map[string]*protos.TableMapping, len(tableMappings))
	for _, tm := range tableMappings {
		mappingByDst[tm.DestinationTableIdentifier] = tm
	}

	var deltas []*protos.TableSchemaDelta
	for dstName, latestSchema := range latest {
		mapping, ok := mappingByDst[dstName]
		if !ok {
			continue
		}

		currentSchema, ok := current[dstName]
		if !ok || currentSchema == nil || latestSchema == nil {
			continue
		}

		currentCols := columnsToMap(currentSchema.Columns)
		latestCols := columnsToMap(latestSchema.Columns)

		var added []*protos.FieldDescription
		for colName, latestCol := range latestCols {
			if existing, ok := currentCols[colName]; !ok {
				added = append(added, cloneFieldDescription(latestCol))
			} else if !fieldDescriptionsEqual(existing, latestCol) {
				added = append(added, cloneFieldDescription(latestCol))
			}
		}

		var dropped []*protos.FieldDescription
		for colName, currentCol := range currentCols {
			if latestCol, ok := latestCols[colName]; !ok {
				dropped = append(dropped, cloneFieldDescription(currentCol))
			} else if !fieldDescriptionsEqual(currentCol, latestCol) {
				dropped = append(dropped, cloneFieldDescription(currentCol))
			}
		}

		if len(added) == 0 && len(dropped) == 0 {
			continue
		}

		deltas = append(deltas, &protos.TableSchemaDelta{
			SrcTableName:    mapping.SourceTableIdentifier,
			DstTableName:    dstName,
			AddedColumns:    added,
			DroppedColumns:  dropped,
			System:          latestSchema.System,
			NullableEnabled: latestSchema.NullableEnabled,
		})
	}

	return deltas
}
