SELECT t.memberID,
       t.courseID,
       t.status,
       t.reviewComment,
       t.progress,
       t.completedAt,
       t.startedAt,
       t.lastActivityAt,
       t.lastReviewedAt,
       t.lastReviewedBy,
       t.createdAt,
       t.updatedAt,
       t.requestID,
       t.learningJournalURL
FROM edex_courses.Enrollment t;