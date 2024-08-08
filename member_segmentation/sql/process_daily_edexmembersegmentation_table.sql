/*
CREATE TABLE ex_application.tmp_segmentation_update (
  `memberId` char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  `class` char(36) CHARACTER SET ascii DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
*/

delete from ex_application.tmp_segmentation_update;

insert into ex_application.tmp_segmentation_update
select t.memberId, t.class, t.rule 
from ex_application.MemberSegmentation t
where t.class = 'K12'
and t.memberId in (select s.memberId from ex_application.EdExMemberSegmentation s where s.class != 'K12');

insert into ex_application.tmp_segmentation_update
select t.memberId, t.class, t.rule
from ex_application.MemberSegmentation t
where t.class = 'HED' 
and t.memberId in (select s.memberId from ex_application.EdExMemberSegmentation s where s.class != 'HED');

UPDATE ex_application.EdExMemberSegmentation 
SET ex_application.EdExMemberSegmentation.class = 'K12', ex_application.EdExMemberSegmentation.processed = FALSE
WHERE memberId IN(SELECT memberId FROM (SELECT memberId FROM ex_application.tmp_segmentation_update m where m.class='K12') abc);

UPDATE ex_application.EdExMemberSegmentation 
SET ex_application.EdExMemberSegmentation.class = 'HED', ex_application.EdExMemberSegmentation.processed = FALSE
WHERE memberId IN(SELECT memberId FROM (SELECT memberId FROM ex_application.tmp_segmentation_update m where m.class='HED') abc);

INSERT INTO ex_application.EdExMemberSegmentation
SELECT t.memberId,
       t.classDesc,
       t.class,
       t.rule,
       t.createdAt,
       false as processed
FROM ex_application.MemberSegmentation t
WHERE t.memberId not in (select t1.memberId from ex_application.EdExMemberSegmentation t1);