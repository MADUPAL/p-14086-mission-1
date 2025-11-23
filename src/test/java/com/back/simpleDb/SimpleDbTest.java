package com.back.simpleDb;

import org.junit.jupiter.api.*;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class SimpleDbTest {
    private static SimpleDb simpleDb;

    @BeforeAll//테스트전에 한번만 -> 공통 데이터 세팅
    public static void beforeAll() {
        simpleDb = new SimpleDb("localhost", "root", "lldj123414", "simpleDb__test");
        simpleDb.setDevMode(true);

        createArticleTable();
    }

    private static void createArticleTable() {
        simpleDb.run("DROP TABLE IF EXISTS article");

        simpleDb.run("""
                CREATE TABLE article (
                    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    PRIMARY KEY(id),
                    createdDate DATETIME NOT NULL,
                    modifiedDate DATETIME NOT NULL,
                    title VARCHAR(100) NOT NULL,
                    `body` TEXT NOT NULL,
                    isBlind BIT(1) NOT NULL DEFAULT 0
                )
                """);
    }

    private void truncateArticleTable() {
        simpleDb.run("TRUNCATE article");
    }

    private void makeArticleTestData() {
        IntStream.rangeClosed(1, 6).forEach(no -> {
            boolean isBlind = no > 3;
            String title = "제목%d".formatted(no);
            String body = "내용%d".formatted(no);

            simpleDb.run("""
                    INSERT INTO article
                    SET createdDate = NOW(),
                    modifiedDate = NOW(),
                    title = ?,
                    `body` = ?,
                    isBlind = ?
                    """, title, body, isBlind);
        });
    }

    @BeforeEach//테스트마다실행 -> 개별 테스트에서 만든 흔적들을 정리 할 수 있다.
    public void beforeEach() {
        truncateArticleTable();
        makeArticleTestData();
    }

    @Test
    void t000() {

    }

    @Test
    @DisplayName("insert")
    public void t001() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        INSERT INTO article
        SET createdDate = NOW() ,
        modifiedDate = NOW() ,
        title = '제목 new' ,
        body = '내용 new'
        */
        sql.append("INSERT INTO article")
                .append("SET createdDate = NOW()")
                .append(", modifiedDate = NOW()")
                .append(", title = ?", "제목 new")
                .append(", body = ?", "내용 new");

        long newId = sql.insert(); // AUTO_INCREMENT 에 의해서 생성된 주키 리턴

        assertThat(newId).isGreaterThan(0);
    }

    @Test
    @DisplayName("update")
    public void t002() {
        Sql sql = simpleDb.genSql();

        // id가 0, 1, 2, 3인 글 수정
        // id가 0인 글은 없으니, 실제로는 3개의 글이 삭제됨

        /*
        == rawSql ==
        UPDATE article
        SET title = '제목 new'
        WHERE id IN ('0', '1', '2', '3')
        */
        sql.append("UPDATE article")
                .append("SET title = ?", "제목 new")
                .append("WHERE id IN (?, ?, ?, ?)", 0, 1, 2, 3);

        // 수정된 row 개수
        int affectedRowsCount = sql.update();

        assertThat(affectedRowsCount).isEqualTo(3);
    }
    @Test
    @DisplayName("update")
    public void t002_1() {
        Sql sql = simpleDb.genSql();

        // id가 0, 1, 2, 3인 글 수정
        // id가 0인 글은 없으니, 실제로는 3개의 글이 삭제됨

        /*
        == rawSql ==
        UPDATE article
        SET title = '제목 new'
        WHERE id IN ('0', '1', '2', '3')
        */
        sql.append("UPDATE article")
                .append("SET title = ?", "제목 new")
                .append("WHERE id = ?", 0);

        // 수정된 row 개수
        int affectedRowsCount = sql.update();

        assertThat(affectedRowsCount).isEqualTo(0);
    }
    @Test
    @DisplayName("delete")
    public void t003() {
        Sql sql = simpleDb.genSql();

        // id가 0, 1, 3인 글 삭제
        // id가 0인 글은 없으니, 실제로는 2개의 글이 삭제됨
        /*
        == rawSql ==
        DELETE FROM article
        WHERE id IN ('0', '1', '3')
        */
        sql.append("DELETE")
                .append("FROM article")
                .append("WHERE id IN (?, ?, ?)", 0, 1, 3);

        // 삭제된 row 개수
        int affectedRowsCount = sql.delete();

        assertThat(affectedRowsCount).isEqualTo(2);
    }
    @Test
    @DisplayName("selectRows")
    public void t004() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT *
        FROM article
        ORDER BY id ASC
        LIMIT 3
        */
        sql.append("SELECT * FROM article ORDER BY id ASC LIMIT 3");
        List<Map<String, Object>> articleRows = sql.selectRows();

        IntStream.range(0, articleRows.size()).forEach(i -> {
            long id = i + 1;

            Map<String, Object> articleRow = articleRows.get(i);

            assertThat(articleRow.get("id")).isEqualTo(id);
            assertThat(articleRow.get("title")).isEqualTo("제목%d".formatted(id));
            assertThat(articleRow.get("body")).isEqualTo("내용%d".formatted(id));
            assertThat(articleRow.get("createdDate")).isInstanceOf(LocalDateTime.class);
            assertThat(articleRow.get("createdDate")).isNotNull();
            assertThat(articleRow.get("modifiedDate")).isInstanceOf(LocalDateTime.class);
            assertThat(articleRow.get("modifiedDate")).isNotNull();
            assertThat(articleRow.get("isBlind")).isEqualTo(false);
        });
    }

    @Test
    @DisplayName("selectRow")
    public void t005() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT *
        FROM article
        WHERE id = 1
        */
        sql.append("SELECT * FROM article WHERE id = 1");
        Map<String, Object> articleRow = sql.selectRow();

        assertThat(articleRow.get("id")).isEqualTo(1L);
        assertThat(articleRow.get("title")).isEqualTo("제목1");
        assertThat(articleRow.get("body")).isEqualTo("내용1");
        assertThat(articleRow.get("createdDate")).isInstanceOf(LocalDateTime.class);
        assertThat(articleRow.get("createdDate")).isNotNull();
        assertThat(articleRow.get("modifiedDate")).isInstanceOf(LocalDateTime.class);
        assertThat(articleRow.get("modifiedDate")).isNotNull();
        assertThat(articleRow.get("isBlind")).isEqualTo(false);
    }

    @Test
    @DisplayName("selectDatetime")
    public void t006() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT NOW()
        */
        sql.append("SELECT NOW()");

        LocalDateTime datetime = sql.selectDatetime();

        long diff = ChronoUnit.SECONDS.between(datetime, LocalDateTime.now());

        assertThat(diff).isLessThanOrEqualTo(1L);
    }
    @Test
    @DisplayName("selectLong")
    public void t007() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT id
        FROM article
        WHERE id = 1
        */
        sql.append("SELECT id")
                .append("FROM article")
                .append("WHERE id = 1");

        Long id = sql.selectLong();

        assertThat(id).isEqualTo(1);
    }

    @Test
    @DisplayName("selectString")
    public void t008() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT title
        FROM article
        WHERE id = 1
        */
        sql.append("SELECT title")
                .append("FROM article")
                .append("WHERE id = 1");

        String title = sql.selectString();

        assertThat(title).isEqualTo("제목1");
    }

    @Test
    @DisplayName("selectBoolean")
    public void t009() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT isBlind
        FROM article
        WHERE id = 1
        */
        sql.append("SELECT isBlind")
                .append("FROM article")
                .append("WHERE id = 1");

        Boolean isBlind = sql.selectBoolean();

        assertThat(isBlind).isEqualTo(false);
    }

    @Test
    @DisplayName("selectBoolean, 2nd")
    public void t010() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT 1 = 1
        */
        sql.append("SELECT 1 = 1");

        Boolean isBlind = sql.selectBoolean();

        assertThat(isBlind).isEqualTo(true);
    }

    @Test
    @DisplayName("selectBoolean, 3rd")
    public void t011() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT 1 = 0
        */
        sql.append("SELECT 1 = 0");

        Boolean isBlind = sql.selectBoolean();

        assertThat(isBlind).isEqualTo(false);
    }

    @Test
    @DisplayName("select, LIKE 사용법")
    public void t012() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT COUNT(*)
        FROM article
        WHERE id BETWEEN '1' AND '3'
        AND title LIKE CONCAT('%', '제목' '%')
        */
        sql.append("SELECT COUNT(*)")
                .append("FROM article")
                .append("WHERE id BETWEEN ? AND ?", 1, 3)
                .append("AND title LIKE CONCAT('%', ? '%')", "제목");

        long count = sql.selectLong();

        assertThat(count).isEqualTo(3);
    }

    @Test
    @DisplayName("appendIn")
    public void t013() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT COUNT(*)
        FROM article
        WHERE id IN ('1', '2', '3')
        */
        sql.append("SELECT COUNT(*)")
                .append("FROM article")
                .appendIn("WHERE id IN (?)", 1, 2, 3);

        long count = sql.selectLong();

        assertThat(count).isEqualTo(3);
    }
    @Test
    @DisplayName("selectLongs, ORDER BY FIELD 사용법")
    public void t014() {
        Long[] ids = new Long[]{2L, 1L, 3L};

        Sql sql = simpleDb.genSql();
        /*
        SELECT id
        FROM article
        WHERE id IN ('2', '3', '1')
        ORDER BY FIELD (id, '2', '3', '1')
        */
        sql.append("SELECT id")
                .append("FROM article")
                .appendIn("WHERE id IN (?)", ids)
                .appendIn("ORDER BY FIELD (id, ?)", ids);

        List<Long> foundIds = sql.selectLongs();

        assertThat(foundIds).isEqualTo(Arrays.stream(ids).toList());
    }

    @Test
    @DisplayName("selectRows, Article")
    public void t015() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT *
        FROM article
        ORDER BY id ASC
        LIMIT 3
        */
        sql.append("SELECT * FROM article ORDER BY id ASC LIMIT 3");
        List<Article> articleRows = sql.selectRows(Article.class);

        IntStream.range(0, articleRows.size()).forEach(i -> {
            long id = i + 1;

            Article article = articleRows.get(i);

            assertThat(article.getId()).isEqualTo(id);
            assertThat(article.getTitle()).isEqualTo("제목%d".formatted(id));
            assertThat(article.getBody()).isEqualTo("내용%d".formatted(id));
            assertThat(article.getCreatedDate()).isInstanceOf(LocalDateTime.class);
            assertThat(article.getCreatedDate()).isNotNull();
            assertThat(article.getModifiedDate()).isInstanceOf(LocalDateTime.class);
            assertThat(article.getModifiedDate()).isNotNull();
            assertThat(article.isBlind()).isEqualTo(false);
        });
    }

    @Test
    @DisplayName("selectRow, Article")
    public void t016() {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT *
        FROM article
        WHERE id = 1
        */
        sql.append("SELECT * FROM article WHERE id = 1");
        Article article = sql.selectRow(Article.class);

        Long id = 1L;

        assertThat(article.getId()).isEqualTo(id);
        assertThat(article.getTitle()).isEqualTo("제목%d".formatted(id));
        assertThat(article.getBody()).isEqualTo("내용%d".formatted(id));
        assertThat(article.getCreatedDate()).isInstanceOf(LocalDateTime.class);
        assertThat(article.getCreatedDate()).isNotNull();
        assertThat(article.getModifiedDate()).isInstanceOf(LocalDateTime.class);
        assertThat(article.getModifiedDate()).isNotNull();
        assertThat(article.isBlind()).isEqualTo(false);
    }

    @Test
    @DisplayName("use in multi threading")
    public void t017() throws InterruptedException {
        // 쓰레드 풀의 크기를 정의합니다.
        int numberOfThreads = 10;

        // 고정 크기의 쓰레드 풀을 생성합니다.
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        // 성공한 작업의 수를 세는 원자적 카운터를 생성합니다.
        AtomicInteger successCounter = new AtomicInteger(0);

        // 동시에 실행되는 작업의 수를 세는 데 사용되는 래치를 생성합니다.
        CountDownLatch latch = new CountDownLatch(numberOfThreads);

        // 각 쓰레드에서 실행될 작업을 정의합니다.
        Runnable task = () -> {
            try {
                // SimpleDB에서 SQL 객체를 생성합니다.
                Sql sql = simpleDb.genSql();

                // SQL 쿼리를 작성합니다.
                sql.append("SELECT * FROM article WHERE id = 1");

                // 쿼리를 실행하여 결과를 Article 객체로 매핑합니다.
                Article article = sql.selectRow(Article.class);

                // 기대하는 Article 객체의 ID를 정의합니다.
                Long id = 1L;

                // Article 객체의 값이 기대하는 값과 일치하는지 확인하고,
                // 일치하는 경우 성공 카운터를 증가시킵니다.
                if (article.getId() == id &&
                        article.getTitle().equals("제목%d".formatted(id)) &&
                        article.getBody().equals("내용%d".formatted(id)) &&
                        article.getCreatedDate() != null &&
                        article.getModifiedDate() != null &&
                        !article.isBlind()) {
                    successCounter.incrementAndGet();
                }
            } finally {
                // 커넥션 종료
                simpleDb.close();
                // 작업이 완료되면 래치 카운터를 감소시킵니다.
                latch.countDown();
            }
        };

        // 쓰레드 풀에서 쓰레드를 할당받아 작업을 실행합니다.
        for (int i = 0; i < numberOfThreads; i++) {
            executorService.submit(task);
        }

        // 모든 작업이 완료될 때까지 대기하거나, 최대 10초 동안 대기합니다.
        latch.await(10, TimeUnit.SECONDS);

        // 쓰레드 풀을 종료시킵니다.
        executorService.shutdown();

        // 성공 카운터가 쓰레드 수와 동일한지 확인합니다.
        assertThat(successCounter.get()).isEqualTo(numberOfThreads);
    }

    @Test
    @DisplayName("rollback")
    public void t018() {
        // SimpleDB에서 SQL 객체를 생성합니다.
        long oldCount = simpleDb.genSql()
                .append("SELECT COUNT(*)")
                .append("FROM article")
                .selectLong();

        // 트랜잭션을 시작합니다.
        simpleDb.startTransaction();

        simpleDb.genSql()
                .append("INSERT INTO article ")
                .append("(createdDate, modifiedDate, title, body)")
                .appendIn("VALUES (NOW(), NOW(), ?)", "새 제목", "새 내용")
                .insert();

        simpleDb.rollback();

        long newCount = simpleDb.genSql()
                .append("SELECT COUNT(*)")
                .append("FROM article")
                .selectLong();

        assertThat(newCount).isEqualTo(oldCount);
    }

    @Test
    @DisplayName("commit")
    public void t019() {
        // SimpleDB에서 SQL 객체를 생성합니다.
        long oldCount = simpleDb.genSql()
                .append("SELECT COUNT(*)")
                .append("FROM article")
                .selectLong();

        // 트랜잭션을 시작합니다.
        simpleDb.startTransaction();

        simpleDb.genSql()
                .append("INSERT INTO article ")
                .append("(createdDate, modifiedDate, title, body)")
                .appendIn("VALUES (NOW(), NOW(), ?)", "새 제목", "새 내용")
                .insert();

        simpleDb.commit();

        long newCount = simpleDb.genSql()
                .append("SELECT COUNT(*)")
                .append("FROM article")
                .selectLong();

        assertThat(newCount).isEqualTo(oldCount + 1);
    }

}