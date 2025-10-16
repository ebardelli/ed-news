-- Fixture SQL: combined_articles with 25 rows
BEGIN TRANSACTION;
CREATE TABLE combined_articles (
    doi TEXT,
    title TEXT,
    link TEXT,
    feed_title TEXT,
    content TEXT,
    published TEXT,
    authors TEXT
);

-- 18 articles on 2025-10-16 (most recent date)
INSERT INTO combined_articles (doi, title, link, feed_title, content, published, authors) VALUES
('doi-r1','R1','','Feed X','content','2025-10-16T08:00:00','auth'),
('doi-r2','R2','','Feed X','content','2025-10-16T08:10:00','auth'),
('doi-r3','R3','','Feed X','content','2025-10-16T08:20:00','auth'),
('doi-r4','R4','','Feed X','content','2025-10-16T08:30:00','auth'),
('doi-r5','R5','','Feed X','content','2025-10-16T08:40:00','auth'),
('doi-r6','R6','','Feed X','content','2025-10-16T08:50:00','auth'),
('doi-r7','R7','','Feed X','content','2025-10-16T09:00:00','auth'),
('doi-r8','R8','','Feed X','content','2025-10-16T09:10:00','auth'),
('doi-r9','R9','','Feed X','content','2025-10-16T09:20:00','auth'),
('doi-r10','R10','','Feed X','content','2025-10-16T09:30:00','auth'),
('doi-r11','R11','','Feed X','content','2025-10-16T09:40:00','auth'),
('doi-r12','R12','','Feed X','content','2025-10-16T09:50:00','auth'),
('doi-r13','R13','','Feed X','content','2025-10-16T10:00:00','auth'),
('doi-r14','R14','','Feed X','content','2025-10-16T10:10:00','auth'),
('doi-r15','R15','','Feed X','content','2025-10-16T10:20:00','auth'),
('doi-r16','R16','','Feed X','content','2025-10-16T10:30:00','auth'),
('doi-r17','R17','','Feed X','content','2025-10-16T10:40:00','auth'),
('doi-r18','R18','','Feed X','content','2025-10-16T10:50:00','auth');

-- 7 articles on 2025-10-15 (the 20th article's date)
INSERT INTO combined_articles (doi, title, link, feed_title, content, published, authors) VALUES
('doi-s1','S1','','Feed Y','content','2025-10-15T07:00:00','auth'),
('doi-s2','S2','','Feed Y','content','2025-10-15T07:10:00','auth'),
('doi-s3','S3','','Feed Y','content','2025-10-15T07:20:00','auth'),
('doi-s4','S4','','Feed Y','content','2025-10-15T07:30:00','auth'),
('doi-s5','S5','','Feed Y','content','2025-10-15T07:40:00','auth'),
('doi-s6','S6','','Feed Y','content','2025-10-15T07:50:00','auth'),
('doi-s7','S7','','Feed Y','content','2025-10-15T08:00:00','auth');

COMMIT;
