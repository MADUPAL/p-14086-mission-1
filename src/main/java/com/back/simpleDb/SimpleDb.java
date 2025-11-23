package com.back.simpleDb;

import lombok.Setter;

import java.sql.*;

/**
 *  SimpleDb 역할
 *  1. connection 생성과 관리
 *  2. Sql 객체 생성
 *  3. 트랜잭션 상태 관리
 */
public class SimpleDb {
    private final String url;
    private final String username;
    private final String password;

    @Setter
    private boolean devMode;

    private final ThreadLocal<Connection> txConn = new ThreadLocal<>();

    public SimpleDb(String host, String username, String password, String dbName) {
        this.url = String.format("jdbc:mysql://%s:3306/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Seoul", host, dbName);
        this.username = username;
        this.password = password;
    }

    Connection getConnection() throws SQLException {
        Connection conn = txConn.get();
        if (conn != null) {
            return conn;
        }

        return DriverManager.getConnection(url, username, password);
    }

    boolean isInTransaction() {
        return txConn.get() != null;
    }

    public void run(String sql) {
        if (devMode) {
            System.out.println("SQL: " + sql);
        }

        // try-with-resources를 사용하여 Connection과 Statement를 자동으로 닫습니다.
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(sql);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void run(String sql, Object... params) {
        if (devMode) {
            System.out.println("SQL: " + sql);
            System.out.println("  params: ");
            for (int i = 0; i < params.length; i++) {
                System.out.println("    $" + (i + 1) + " = " + params[i]);
            }
        }

        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }

            pstmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Sql genSql() {
        return new Sql(this);
    }

    public void close() {
    }

    /**
     * transaction 시작 시, 해당 스레드에 있는 연결정보를 ThreadLocal에 넣음
     */
    public void startTransaction() {
        if (isInTransaction()) {
            throw new IllegalStateException("이미 트랜잭션이 시작된 상태입니다.");
        }
        try {
            Connection conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(false);

            txConn.set(conn);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void rollback() {
        Connection conn = txConn.get();
        if(conn == null)
            return;

        try {
            conn.rollback();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                conn.close();
            } catch (SQLException ignore) {}
             // transaction이 끝나면 connection close -> 해당 thread의 연결 정보도 삭제되어야 함
            txConn.remove();
        }
    }

    public void commit() {
        Connection conn = txConn.get();
        if(conn == null)
            return;

        try {
            conn.commit();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                conn.close();
            } catch (SQLException ignore) {}
            // transaction이 끝나면 connection close -> 해당 thread의 연결 정보도 삭제되어야 함
            txConn.remove();
        }
    }
}
