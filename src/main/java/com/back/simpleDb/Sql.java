package com.back.simpleDb;

import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;

/**
 *  Sql 역할
 *  1. Sql 문자열 빌더
 *  2. 파라미터 목록 관리
 *  3. 쿼리 실행
 *  4. 커넥션 사용/정리 정책
 */
public class Sql {
    private final SimpleDb simpleDb;
    private final StringBuilder sql = new StringBuilder();
    private final List<Object> params = new ArrayList<>();

    public Sql(SimpleDb simpleDb) {
        this.simpleDb = simpleDb;
    }

    //Fluent API
    //this를 return
    public Sql append(String queryString) {
        if(queryString == null || queryString.isBlank())
            return this;

        if (!sql.isEmpty()) {
            sql.append(" ");
        }
        sql.append(queryString);

        return this;
    }

    public Sql append(String queryString, Object... param) {
        append(queryString);
        for (Object p : param) {
            params.add(p);
        }
        return this;
    }

    public Sql appendIn(String queryString, Object... param) {
        if(queryString == null || queryString.isBlank())
            return this;

        int idx = queryString.indexOf("?");
        if (idx == -1) {
            throw new IllegalArgumentException("IN 절에 [ ? ] 가 없습니다.");
        }
        String placeholder = String.join(", ", Collections.nCopies(param.length, "?"));
        String replaced = queryString.replace("?", placeholder);

        if (!sql.isEmpty()) {
            sql.append(" ");
        }

        sql.append(replaced);

        for (Object p : param) {
            params.add(p);
        }

        return this;
    }

    public long insert() {
        String rawSql = getRawSqlOrThrow();

        return withConnection(conn-> {
            try (PreparedStatement pstmt = conn.prepareStatement(rawSql, Statement.RETURN_GENERATED_KEYS)) {
                bindParams(pstmt);
                pstmt.executeUpdate();

                //rs는 resource 반납해야함
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    if (rs.next()) {
                        return rs.getLong(1);
                    }
                }
                return 0L;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public int update() {
        String rawSql = getRawSqlOrThrow();

        return withConnection(conn -> {
            try (PreparedStatement pstmt = conn.prepareStatement(rawSql)) {
                bindParams(pstmt);
                return pstmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public int delete() {
        String rawSql = getRawSqlOrThrow();

        return withConnection(conn -> {
            try(PreparedStatement pstmt = conn.prepareStatement(rawSql)) {
                bindParams(pstmt);

                return pstmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    //select -> executeQuery()사용

    public List<Map<String, Object>> selectRows() {
        String rawSql = getRawSqlOrThrow();

        return withConnection(conn -> {

            try(PreparedStatement pstmt = conn.prepareStatement(rawSql)) {
                    bindParams(pstmt);


                try (ResultSet rs = pstmt.executeQuery()) {
                    List<Map<String, Object>> rows = new ArrayList<>();

                    ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();

                    while (rs.next()) {

                        Map<String, Object> row = new LinkedHashMap<>();

                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = meta.getColumnLabel(i); //as 없으면 getColumnName값을 반환
                            Object value = rs.getObject(i);

                            if (value instanceof Timestamp ts) {
                                value = ts.toLocalDateTime();
                            }

                            row.put(columnName, value);
                        }
                        rows.add(row);
                    }

                    return rows;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

    }
    /**
     * 리플렉션으로 객체 생성 후 객체리스트반환
     */
    public <T> List<T> selectRows(Class<T> clazz) {
        List<Map<String, Object>> rows = selectRows();
        List<T> result = new ArrayList<>();

        for (Map<String, Object> row : rows) {
            try {
                T obj = clazz.getDeclaredConstructor().newInstance();

                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    String column = entry.getKey();
                    Object value = entry.getValue();

                    String fieldName = toFieldName(column);

                    try {
                        Field field = clazz.getDeclaredField(fieldName);
                        field.setAccessible(true);

                        Object converted = convertValue(field.getType(), value);

                        field.set(obj, converted);
                    } catch (NoSuchFieldException ignore) {

                    }
                }
                result.add(obj);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }

    public Map<String, Object> selectRow() {
        List<Map<String, Object>> rows = selectRows();
        if(rows.isEmpty())
            return new LinkedHashMap<>();

        return rows.get(0);
    }

    public <T> T selectRow(Class<T> clazz) {
        List<T> objs = selectRows(clazz);
        if(objs.isEmpty())
            return null;

        return objs.get(0);
    }

    public LocalDateTime selectDatetime() {
        Object value = selectSingleValue();

        if(value == null) {
            return null;
        }
        if (value instanceof Timestamp ts) {
            return ts.toLocalDateTime();
        }
        if (value instanceof LocalDateTime ldt) {
            return ldt;
        }
        throw new RuntimeException("DateTime 형식이 아닙니다: " + value);
    }

    public Long selectLong() {
        Object value = selectSingleValue();

        if(value == null) {
            return null;
        }
        if (value instanceof Number num)  // Number: Integer, Long, BigDecimal 다 허용
            return num.longValue();

        throw new RuntimeException("숫자가 아닙니다: " + value);
    }

    public String selectString() {
        Object value = selectSingleValue();

        if (value == null) {
            return null;
        }

        if (value instanceof String string) {
            return string;
        }

        throw new RuntimeException("문자열이 아닙니다: " + value);

    }

    /**
     *  DB에 따라
     *  Number, Boolean으로 반환함
     */
    public Boolean selectBoolean() {
        Object value = selectSingleValue();

        if (value == null) {
            return null;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        if (value instanceof Number num) {
            return num.intValue() != 0;
        }

        throw new RuntimeException("Boolean형식이 아닙니다: " + value);

    }

    public List<Long> selectLongs() {
        List<Map<String, Object>> rows = selectRows();
        ArrayList<Long> result = new ArrayList<>(rows.size());

        for (Map<String, Object> row : rows) {
            if(row.isEmpty()){
                result.add(null);
                continue;
            }

            Object value = row.values().iterator().next(); //id값만 찾기

            if (value == null) {
                result.add(null);
                continue;
            }

            if (value instanceof Number num) {
                result.add(num.longValue());
            } else {
                throw new RuntimeException("숫자 타입이 아닙니다: " + value);
            }
        }
        return result;
    }

    private Object selectSingleValue() {
        String rawSql = getRawSqlOrThrow();

        return withConnection(conn-> {
            try(PreparedStatement pstmt = conn.prepareStatement(rawSql)) {
                // 파라미터 바인딩
                bindParams(pstmt);

                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    return rs.getObject(1);
                }
            } catch(SQLException e) {
                throw new RuntimeException(e);
            }
        });

    }

    /**
     * snake_case_column
     * to
     * camelCaseColumn
     */
    private String toFieldName(String column) {
        if(!column.contains("_"))
            return column;

        StringBuilder sb = new StringBuilder();
        String[] splits = column.split("_");

        sb.append(splits[0].toLowerCase());

        for (int i = 1; i < splits.length; i++) {
            String str = splits[i];
            sb.append(Character.toUpperCase(str.charAt(0)));
            sb.append(str.substring(1).toLowerCase());
        }

        return sb.toString();
    }

    private Object convertValue(Class<?> type, Object value) {
        if (value == null)
            return null;

        if (type.isAssignableFrom(value.getClass())) {
            return value;
        }

        if (type == long.class || type == Long.class) {
            return ((Number) value).longValue();
        }

        if (type == int.class || type == Integer.class) {
            return ((Number) value).intValue();
        }

        if (type == boolean.class || type == Boolean.class) {
            if (value instanceof Boolean b) return b;
            if (value instanceof Number n) return n.intValue() != 0;
        }

        if (type == String.class) {
            return value.toString();
        }

        if (type == LocalDateTime.class && value instanceof Timestamp ts) {
            return ts.toLocalDateTime();
        }

        throw new RuntimeException("변환 불가: " + value + " → " + type);
    }

    private String getRawSqlOrThrow() {
        String rawSql = sql.toString();

        if (rawSql.isBlank()) {
            throw new IllegalStateException("SQL이 비어 있습니다. append(...)로 먼저 쿼리를 구성하세요.");
        }
        return rawSql;
    }

    private void bindParams(PreparedStatement pstmt) throws SQLException {
        for(int i = 0; i < params.size(); i++) {
            pstmt.setObject(i+1, params.get(i));
        }
    }

    /**
     * 1. simpleDb.getConnection()으로 커넥션 가져오기
     * 2. simpleDb.isInTransaction()으로 트랜잭션 여부 확인
     * 3. PreparedStatement 만들고 파라미터 바인딩하고 execute
     * 4. 트랜잭션이 아니면 커넥션 닫기, 트랜잭션이면 안 닫기
     * 5. SQLException → RuntimeException으로 감싸서 던지기
     * 1~5를 템플릿 함수로 만듦
     * 실제 쿼리 실행 로직만 콜백으로 넘기기 <Connection, T> -> Connection타입을 받고 T타입을 리턴하는 함수
     */
    private <T> T withConnection(Function<Connection, T> callback) {
        Connection conn = null;
        try {
            conn = simpleDb.getConnection();
            boolean inTx = simpleDb.isInTransaction();

            try {
                return callback.apply(conn);
            } finally {
                if (!inTx && conn != null) {
                    try { conn.close(); } catch (SQLException ignore) {}
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
