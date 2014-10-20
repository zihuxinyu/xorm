package xorm

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-xorm/core"
)

// func init() {
// 	RegisterDialect("postgres", &postgres{})
// }
// from http://www.postgresql.org/docs/current/static/sql-keywords-appendix.html
var (
	postgresReservedWords = map[string]bool{
		"ABS":                   true,
		"ABSOLUTE":              true,
		"ACTION":                true,
		"ADD":                   true,
		"ALL":                   true,
		"ALLOCATE":              true,
		"ALTER":                 true,
		"ANALYSE":               true,
		"ANALYZE":               true,
		"AND":                   true,
		"ANY":                   true,
		"ARE":                   true,
		"ARRAY":                 true,
		"ARRAY_AGG":             true,
		"ARRAY_MAX_CARDINALITY": true,
		"AS":                               true,
		"ASC":                              true,
		"ASENSITIVE":                       true,
		"ASSERTION":                        true,
		"ASYMMETRIC":                       true,
		"AT":                               true,
		"ATOMIC":                           true,
		"AUTHORIZATION":                    true,
		"AVG":                              true,
		"BEGIN":                            true,
		"BEGIN_FRAME":                      true,
		"BEGIN_PARTITION":                  true,
		"BINARY":                           true,
		"BIT_LENGTH":                       true,
		"BLOB":                             true,
		"BOTH":                             true,
		"BY":                               true,
		"CALL":                             true,
		"CALLED":                           true,
		"CARDINALITY":                      true,
		"CASCADE":                          true,
		"CASCADED":                         true,
		"CASE":                             true,
		"CAST":                             true,
		"CATALOG":                          true,
		"CEIL":                             true,
		"CEILING":                          true,
		"CHARACTER_LENGTH":                 true,
		"CHAR_LENGTH":                      true,
		"CHECK":                            true,
		"CLOB":                             true,
		"CLOSE":                            true,
		"COLLATE":                          true,
		"COLLATION":                        true,
		"COLLECT":                          true,
		"COLUMN":                           true,
		"COMMIT":                           true,
		"CONCURRENTLY":                     true,
		"CONDITION":                        true,
		"CONNECT":                          true,
		"CONNECTION":                       true,
		"CONSTRAINT":                       true,
		"CONSTRAINTS":                      true,
		"CONTAINS":                         true,
		"CONTINUE":                         true,
		"CONVERT":                          true,
		"CORR":                             true,
		"CORRESPONDING":                    true,
		"COUNT":                            true,
		"COVAR_POP":                        true,
		"COVAR_SAMP":                       true,
		"CREATE":                           true,
		"CROSS":                            true,
		"CUBE":                             true,
		"CUME_DIST":                        true,
		"CURRENT":                          true,
		"CURRENT_CATALOG":                  true,
		"CURRENT_DATE":                     true,
		"CURRENT_DEFAULT_TRANSFORM_GROUP":  true,
		"CURRENT_PATH":                     true,
		"CURRENT_ROLE":                     true,
		"CURRENT_ROW":                      true,
		"CURRENT_SCHEMA":                   true,
		"CURRENT_TIME":                     true,
		"CURRENT_TIMESTAMP":                true,
		"CURRENT_TRANSFORM_GROUP_FOR_TYPE": true,
		"CURRENT_USER":                     true,
		"CURSOR":                           true,
		"CYCLE":                            true,
		"DATALINK":                         true,
		"DATE":                             true,
		"DAY":                              true,
		"DEALLOCATE":                       true,
		"DECLARE":                          true,
		"DEFAULT":                          true,
		"DEFERRABLE":                       true,
		"DEFERRED":                         true,
		"DELETE":                           true,
		"DENSE_RANK":                       true,
		"DEREF":                            true,
		"DESC":                             true,
		"DESCRIBE":                         true,
		"DESCRIPTOR":                       true,
		"DETERMINISTIC":                    true,
		"DIAGNOSTICS":                      true,
		"DISCONNECT":                       true,
		"DISTINCT":                         true,
		"DLNEWCOPY":                        true,
		"DLPREVIOUSCOPY":                   true,
		"DLURLCOMPLETE":                    true,
		"DLURLCOMPLETEONLY":                true,
		"DLURLCOMPLETEWRITE":               true,
		"DLURLPATH":                        true,
		"DLURLPATHONLY":                    true,
		"DLURLPATHWRITE":                   true,
		"DLURLSCHEME":                      true,
		"DLURLSERVER":                      true,
		"DLVALUE":                          true,
		"DO":                               true,
		"DOMAIN":                           true,
		"DOUBLE":                           true,
		"DROP":                             true,
		"DYNAMIC":                          true,
		"EACH":                             true,
		"ELEMENT":                          true,
		"ELSE":                             true,
		"END":                              true,
		"END-EXEC":                         true,
		"END_FRAME":                        true,
		"END_PARTITION":                    true,
		"EQUALS":                           true,
		"ESCAPE":                           true,
		"EVERY":                            true,
		"EXCEPT":                           true,
		"EXCEPTION":                        true,
		"EXEC":                             true,
		"EXECUTE":                          true,
		"EXP":                              true,
		"EXTERNAL":                         true,
		"FALSE":                            true,
		"FETCH":                            true,
		"FILTER":                           true,
		"FIRST":                            true,
		"FIRST_VALUE":                      true,
		"FLOOR":                            true,
		"FOR":                              true,
		"FOREIGN":                          true,
		"FOUND":                            true,
		"FRAME_ROW":                        true,
		"FREE":                             true,
		"FREEZE":                           true,
		"FROM":                             true,
		"FULL":                             true,
		"FUNCTION":                         true,
		"FUSION":                           true,
		"GET":                              true,
		"GLOBAL":                           true,
		"GO":                               true,
		"GOTO":                             true,
		"GRANT":                            true,
		"GROUP":                            true,
		"GROUPING":                         true,
		"GROUPS":                           true,
		"HAVING":                           true,
		"HOLD":                             true,
		"HOUR":                             true,
		"IDENTITY":                         true,
		"ILIKE":                            true,
		"IMMEDIATE":                        true,
		"IMPORT":                           true,
		"IN":                               true,
		"INDICATOR":                        true,
		"INITIALLY":                        true,
		"INNER":                            true,
		"INPUT":                            true,
		"INSENSITIVE":                      true,
		"INSERT":                           true,
		"INTERSECT":                        true,
		"INTERSECTION":                     true,
		"INTO":                             true,
		"IS":                               true,
		"ISNULL":                           true,
		"ISOLATION":                        true,
		"JOIN":                             true,
		"KEY":                              true,
		"LAG":                              true,
		"LANGUAGE":                         true,
		"LARGE":                            true,
		"LAST":                             true,
		"LAST_VALUE":                       true,
		"LATERAL":                          true,
		"LEAD":                             true,
		"LEADING":                          true,
		"LEFT":                             true,
		"LEVEL":                            true,
		"LIKE":                             true,
		"LIKE_REGEX":                       true,
		"LIMIT":                            true,
		"LN":                               true,
		"LOCAL":                            true,
		"LOCALTIME":                        true,
		"LOCALTIMESTAMP":                   true,
		"LOWER":                            true,
		"MATCH":                            true,
		"MAX":                              true,
		"MAX_CARDINALITY":                  true,
		"MEMBER":                           true,
		"MERGE":                            true,
		"METHOD":                           true,
		"MIN":                              true,
		"MINUTE":                           true,
		"MOD":                              true,
		"MODIFIES":                         true,
		"MODULE":                           true,
		"MONTH":                            true,
		"MULTISET":                         true,
		"NAMES":                            true,
		"NATURAL":                          true,
		"NCLOB":                            true,
		"NEW":                              true,
		"NEXT":                             true,
		"NO":                               true,
		"NORMALIZE":                        true,
		"NOT":                              true,
		"NOTNULL":                          true,
		"NTH_VALUE":                        true,
		"NTILE":                            true,
		"NULL":                             true,
		"OCCURRENCES_REGEX":                true,
		"OCTET_LENGTH":                     true,
		"OF":                               true,
		"OFFSET":                           true,
		"OLD":                              true,
		"ON":                               true,
		"ONLY":                             true,
		"OPEN":                             true,
		"OPTION":                           true,
		"OR":                               true,
		"ORDER":                            true,
		"OUTER":                            true,
		"OUTPUT":                           true,
		"OVER":                             true,
		"OVERLAPS":                         true,
		"PAD":                              true,
		"PARAMETER":                        true,
		"PARTIAL":                          true,
		"PARTITION":                        true,
		"PERCENT":                          true,
		"PERCENTILE_CONT":                  true,
		"PERCENTILE_DISC":                  true,
		"PERCENT_RANK":                     true,
		"PERIOD":                           true,
		"PLACING":                          true,
		"PORTION":                          true,
		"POSITION_REGEX":                   true,
		"POWER":                            true,
		"PRECEDES":                         true,
		"PREPARE":                          true,
		"PRESERVE":                         true,
		"PRIMARY":                          true,
		"PRIOR":                            true,
		"PRIVILEGES":                       true,
		"PROCEDURE":                        true,
		"PUBLIC":                           true,
		"RANGE":                            true,
		"RANK":                             true,
		"READ":                             true,
		"READS":                            true,
		"RECURSIVE":                        true,
		"REF":                              true,
		"REFERENCES":                       true,
		"REFERENCING":                      true,
		"REGR_AVGX":                        true,
		"REGR_AVGY":                        true,
		"REGR_COUNT":                       true,
		"REGR_INTERCEPT":                   true,
		"REGR_R2":                          true,
		"REGR_SLOPE":                       true,
		"REGR_SXX":                         true,
		"REGR_SXY":                         true,
		"REGR_SYY":                         true,
		"RELATIVE":                         true,
		"RELEASE":                          true,
		"RESTRICT":                         true,
		"RESULT":                           true,
		"RETURN":                           true,
		"RETURNING":                        true,
		"RETURNS":                          true,
		"REVOKE":                           true,
		"RIGHT":                            true,
		"ROLLBACK":                         true,
		"ROLLUP":                           true,
		"ROWS":                             true,
		"ROW_NUMBER":                       true,
		"SAVEPOINT":                        true,
		"SCHEMA":                           true,
		"SCOPE":                            true,
		"SCROLL":                           true,
		"SEARCH":                           true,
		"SECOND":                           true,
		"SECTION":                          true,
		"SELECT":                           true,
		"SENSITIVE":                        true,
		"SESSION":                          true,
		"SESSION_USER":                     true,
		"SET":                              true,
		"SIMILAR":                          true,
		"SIZE":                             true,
		"SOME":                             true,
		"SPACE":                            true,
		"SPECIFIC":                         true,
		"SPECIFICTYPE":                     true,
		"SQL":                              true,
		"SQLCODE":                          true,
		"SQLERROR":                         true,
		"SQLEXCEPTION":                     true,
		"SQLSTATE":                         true,
		"SQLWARNING":                       true,
		"SQRT":                             true,
		"START":                            true,
		"STATIC":                           true,
		"STDDEV_POP":                       true,
		"STDDEV_SAMP":                      true,
		"SUBMULTISET":                      true,
		"SUBSTRING_REGEX":                  true,
		"SUCCEEDS":                         true,
		"SUM":                              true,
		"SYMMETRIC":                        true,
		"SYSTEM":                           true,
		"SYSTEM_TIME":                      true,
		"SYSTEM_USER":                      true,
		"TABLE":                            true,
		"TABLESAMPLE":                      true,
		"TEMPORARY":                        true,
		"THEN":                             true,
		"TIMEZONE_HOUR":                    true,
		"TIMEZONE_MINUTE":                  true,
		"TO":                               true,
		"TRAILING":                         true,
		"TRANSACTION":                      true,
		"TRANSLATE":                        true,
		"TRANSLATE_REGEX":                  true,
		"TRANSLATION":                      true,
		"TRIGGER":                          true,
		"TRIM_ARRAY":                       true,
		"TRUE":                             true,
		"TRUNCATE":                         true,
		"UESCAPE":                          true,
		"UNION":                            true,
		"UNIQUE":                           true,
		"UNKNOWN":                          true,
		"UNNEST":                           true,
		"UPDATE":                           true,
		"UPPER":                            true,
		"USAGE":                            true,
		"USER":                             true,
		"USING":                            true,
		"VALUE":                            true,
		"VALUE_OF":                         true,
		"VARBINARY":                        true,
		"VARIADIC":                         true,
		"VARYING":                          true,
		"VAR_POP":                          true,
		"VAR_SAMP":                         true,
		"VERBOSE":                          true,
		"VERSIONING":                       true,
		"VIEW":                             true,
		"WHEN":                             true,
		"WHENEVER":                         true,
		"WHERE":                            true,
		"WIDTH_BUCKET":                     true,
		"WINDOW":                           true,
		"WITH":                             true,
		"WITHIN":                           true,
		"WITHOUT":                          true,
		"WORK":                             true,
		"WRITE":                            true,
		"XML":                              true,
		"XMLAGG":                           true,
		"XMLBINARY":                        true,
		"XMLCAST":                          true,
		"XMLCOMMENT":                       true,
		"XMLDOCUMENT":                      true,
		"XMLITERATE":                       true,
		"XMLNAMESPACES":                    true,
		"XMLQUERY":                         true,
		"XMLTABLE":                         true,
		"XMLTEXT":                          true,
		"XMLVALIDATE":                      true,
		"YEAR":                             true,
		"ZONE":                             true,
	}
)

type postgres struct {
	core.BaseDialect
}

func (db *postgres) Init(d *core.DB, uri *core.Uri, drivername, dataSourceName string) error {
	return db.BaseDialect.Init(d, db, uri, drivername, dataSourceName)
}

func (db *postgres) SqlType(c *core.Column) string {
	var res string
	switch t := c.SQLType.Name; t {
	case core.TinyInt:
		res = core.SmallInt
		return res
	case core.MediumInt, core.Int, core.Integer:
		if c.IsAutoIncrement {
			return core.Serial
		}
		return core.Integer
	case core.Serial, core.BigSerial:
		c.IsAutoIncrement = true
		c.Nullable = false
		res = t
	case core.Binary, core.VarBinary:
		return core.Bytea
	case core.DateTime:
		res = core.TimeStamp
	case core.TimeStampz:
		return "timestamp with time zone"
	case core.Float:
		res = core.Real
	case core.TinyText, core.MediumText, core.LongText:
		res = core.Text
	case core.Uuid:
		res = core.Uuid
	case core.Blob, core.TinyBlob, core.MediumBlob, core.LongBlob:
		return core.Bytea
	case core.Double:
		return "DOUBLE PRECISION"
	default:
		if c.IsAutoIncrement {
			return core.Serial
		}
		res = t
	}

	var hasLen1 bool = (c.Length > 0)
	var hasLen2 bool = (c.Length2 > 0)
	if hasLen2 {
		res += "(" + strconv.Itoa(c.Length) + "," + strconv.Itoa(c.Length2) + ")"
	} else if hasLen1 {
		res += "(" + strconv.Itoa(c.Length) + ")"
	}
	return res
}

func (db *postgres) SupportInsertMany() bool {
	return true
}

func (db *postgres) IsReserved(name string) bool {
	_, ok := postgresReservedWords[strings.ToUpper(name)]
	return ok
}

func (db *postgres) Quote(name string) string {
	return fmt.Sprintf("\"%s\"", name)
}

func (db *postgres) CheckedQuote(name string) string {
	if db.IsReserved(name) {
		return db.Quote(name)
	} else {
		return name
	}
}

func (db *postgres) QuoteStr() string {
	return "\""
}

func (db *postgres) AutoIncrStr() string {
	return ""
}

func (db *postgres) SupportEngine() bool {
	return false
}

func (db *postgres) SupportCharset() bool {
	return false
}

func (db *postgres) IndexOnTable() bool {
	return false
}

func (db *postgres) IndexCheckSql(tableName, idxName string) (string, []interface{}) {
	args := []interface{}{tableName, idxName}
	return "SELECT indexname FROM pg_indexes " +
		"WHERE tablename=? AND indexname=?", args
}

func (db *postgres) TableCheckSql(tableName string) (string, []interface{}) {
	args := []interface{}{tableName}
	return "SELECT tablename FROM pg_tables WHERE tablename=?", args
}

/*func (db *postgres) ColumnCheckSql(tableName, colName string) (string, []interface{}) {
	args := []interface{}{tableName, colName}
	return "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ?" +
		" AND column_name = ?", args
}*/

func (db *postgres) ModifyColumnSql(tableName string, col *core.Column) string {
	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
		tableName, col.Name, db.SqlType(col))
}

func (db *postgres) DropIndexSql(tableName string, index *core.Index) string {
	quote := db.Quote
	//var unique string
	var idxName string = index.Name
	if !strings.HasPrefix(idxName, "UQE_") &&
		!strings.HasPrefix(idxName, "IDX_") {
		if index.Type == core.UniqueType {
			idxName = fmt.Sprintf("UQE_%v_%v", tableName, index.Name)
		} else {
			idxName = fmt.Sprintf("IDX_%v_%v", tableName, index.Name)
		}
	}
	return fmt.Sprintf("DROP INDEX %v", quote(idxName))
}

func (db *postgres) IsColumnExist(tableName string, col *core.Column) (bool, error) {
	args := []interface{}{fmt.Sprintf("^%s$", tableName), fmt.Sprintf("^%s$", col.Name)}
	query := "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name~*$1" +
		" AND column_name~*$2"
	rows, err := db.DB().Query(query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}

func (db *postgres) GetColumns(tableName string) ([]string, map[string]*core.Column, error) {
	args := []interface{}{tableName}
	s := `SELECT column_name,column_default,is_nullable,data_type,character_maximum_length,numeric_precision,numeric_precision_radix,
    CASE WHEN p.contype='p' THEN true ELSE false END AS primarykey,
    CASE WHEN p.contype='u' THEN true ELSE false END AS uniquekey
FROM pg_attribute f
    JOIN pg_class c ON c.oid=f.attrelid JOIN pg_type t ON t.oid=f.atttypid
    LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=f.attnum
    LEFT JOIN pg_namespace n ON n.oid=c.relnamespace
    LEFT JOIN pg_constraint p ON p.conrelid=c.oid AND f.attnum = ANY (p.conkey)
    LEFT JOIN pg_class AS g ON p.confrelid=g.oid
    LEFT JOIN INFORMATION_SCHEMA.COLUMNS s ON s.column_name=f.attname AND c.relname=s.table_name
WHERE c.relkind='r'::char AND c.relname=$1 AND f.attnum > 0 ORDER BY f.attnum;`

	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols := make(map[string]*core.Column)
	colSeq := make([]string, 0)

	for rows.Next() {
		col := new(core.Column)
		col.Indexes = make(map[string]bool)

		var colName, isNullable, dataType string
		var maxLenStr, colDefault, numPrecision, numRadix *string
		var isPK, isUnique bool
		err = rows.Scan(&colName, &colDefault, &isNullable, &dataType, &maxLenStr, &numPrecision, &numRadix, &isPK, &isUnique)
		if err != nil {
			return nil, nil, err
		}

		//fmt.Println(args, colName, isNullable, dataType, maxLenStr, colDefault, numPrecision, numRadix, isPK, isUnique)
		var maxLen int
		if maxLenStr != nil {
			maxLen, err = strconv.Atoi(*maxLenStr)
			if err != nil {
				return nil, nil, err
			}
		}

		col.Name = strings.Trim(colName, `" `)

		if colDefault != nil || isPK {
			if isPK {
				col.IsPrimaryKey = true
			} else {
				col.Default = *colDefault
			}
		}

		if colDefault != nil && strings.HasPrefix(*colDefault, "nextval(") {
			col.IsAutoIncrement = true
		}

		col.Nullable = (isNullable == "YES")

		switch dataType {
		case "character varying", "character":
			col.SQLType = core.SQLType{core.Varchar, 0, 0}
		case "timestamp without time zone":
			col.SQLType = core.SQLType{core.DateTime, 0, 0}
		case "timestamp with time zone":
			col.SQLType = core.SQLType{core.TimeStampz, 0, 0}
		case "double precision":
			col.SQLType = core.SQLType{core.Double, 0, 0}
		case "boolean":
			col.SQLType = core.SQLType{core.Bool, 0, 0}
		case "time without time zone":
			col.SQLType = core.SQLType{core.Time, 0, 0}
		default:
			col.SQLType = core.SQLType{strings.ToUpper(dataType), 0, 0}
		}
		if _, ok := core.SqlTypes[col.SQLType.Name]; !ok {
			return nil, nil, errors.New(fmt.Sprintf("unkonw colType %v", dataType))
		}

		col.Length = maxLen

		if col.SQLType.IsText() || col.SQLType.IsTime() {
			if col.Default != "" {
				col.Default = "'" + col.Default + "'"
			} else {
				if col.DefaultIsEmpty {
					col.Default = "''"
				}
			}
		}
		cols[col.Name] = col
		colSeq = append(colSeq, col.Name)
	}

	return colSeq, cols, nil
}

func (db *postgres) GetTables() ([]*core.Table, error) {
	args := []interface{}{}
	s := "SELECT tablename FROM pg_tables WHERE schemaname='public'"

	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]*core.Table, 0)
	for rows.Next() {
		table := core.NewEmptyTable()
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		table.Name = name
		tables = append(tables, table)
	}
	return tables, nil
}

func (db *postgres) GetIndexes(tableName string) (map[string]*core.Index, error) {
	args := []interface{}{tableName}
	s := "SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='public' AND tablename=$1"

	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := make(map[string]*core.Index, 0)
	for rows.Next() {
		var indexType int
		var indexName, indexdef string
		var colNames []string
		err = rows.Scan(&indexName, &indexdef)
		if err != nil {
			return nil, err
		}
		indexName = strings.Trim(indexName, `" `)
		if strings.HasSuffix(indexName, "_pkey") {
			continue
		}
		if strings.HasPrefix(indexdef, "CREATE UNIQUE INDEX") {
			indexType = core.UniqueType
		} else {
			indexType = core.IndexType
		}
		cs := strings.Split(indexdef, "(")
		colNames = strings.Split(cs[1][0:len(cs[1])-1], ",")

		if strings.HasPrefix(indexName, "IDX_"+tableName) || strings.HasPrefix(indexName, "UQE_"+tableName) {
			newIdxName := indexName[5+len(tableName) : len(indexName)]
			if newIdxName != "" {
				indexName = newIdxName
			}
		}

		index := &core.Index{Name: indexName, Type: indexType, Cols: make([]string, 0)}
		for _, colName := range colNames {
			index.Cols = append(index.Cols, strings.Trim(colName, `" `))
		}
		indexes[index.Name] = index
	}
	return indexes, nil
}

func (db *postgres) Filters() []core.Filter {
	return []core.Filter{&core.IdFilter{}, &core.QuoteFilter{}, &core.SeqFilter{"$", 1}}
}
