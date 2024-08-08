-- Adding additional columns in edex tables 
-- dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
-- dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
-- isCurrent char(36)



-- Edex Tables
drop table if exists edx.ResourceToAcademicLevel;
create table edx.ResourceToAcademicLevel (
resourceId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
academicLevels char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
layer char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.ResourceToAcademicLevel_hist;
create table edx.ResourceToAcademicLevel_hist (
resourceId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
academicLevels char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
layer char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


drop table if exists edx.MemberToAcademicLevel;
create table edx.MemberToAcademicLevel (
memberId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
academicLevels char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
-- status varchar(25) CHARACTER SET  ascii,
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.MemberToAcademicLevel_hist;
create table edx.MemberToAcademicLevel_hist (
memberId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
academicLevels char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
-- status varchar(25) CHARACTER SET  ascii,
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


drop table if exists edx.MemberToSchoolType;
create table edx.MemberToSchoolType (
memberId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
schoolTypeID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.MemberToSchoolType_hist;
create table edx.MemberToSchoolType_hist (
memberId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
schoolTypeID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


drop table if exists edx.AcademicLevel;
CREATE TABLE edx.AcademicLevel (
id char(36) CHARACTER SET ascii NOT NULL,
i18nCategory varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
i18nLabel varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
urlLabel varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
eduLevel char(36) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.AcademicLevel_hist;
CREATE TABLE edx.AcademicLevel_hist (
id char(36) CHARACTER SET ascii NOT NULL,
i18nCategory varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
i18nLabel varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
urlLabel varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
eduLevel char(36) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


drop table if exists edx.SchoolType;
CREATE TABLE edx.SchoolType (
id char(36) CHARACTER SET ascii NOT NULL,
i18nLabel varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
schoolCategory char(36) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.SchoolType_hist;
CREATE TABLE edx.SchoolType_hist (
id char(36) CHARACTER SET ascii NOT NULL,
i18nLabel varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
schoolCategory char(36) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


drop table if exists edx.Member;
CREATE TABLE edx.Member (
id char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
adobeGUID varchar(80) COLLATE utf8mb4_unicode_ci,
imsUserId varchar(80) CHARACTER SET ascii DEFAULT NULL,
imsAuthId varchar(80) CHARACTER SET ascii DEFAULT NULL,
firstName varchar(150) COLLATE utf8mb4_unicode_ci,
lastName varchar(150) COLLATE utf8mb4_unicode_ci,
email varchar(80) COLLATE utf8mb4_unicode_ci,
vanityURL varchar(255) COLLATE utf8mb4_unicode_ci,
interests json NOT NULL,
experience json NOT NULL,
countryCode varchar(255) CHARACTER SET ascii,
status varchar(25) CHARACTER SET  ascii,
reputationPoints int(11) DEFAULT '0',
createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.Member_hist;
CREATE TABLE edx.Member_hist (
id char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
adobeGUID varchar(80) COLLATE utf8mb4_unicode_ci,
imsUserId varchar(80) CHARACTER SET ascii DEFAULT NULL,
imsAuthId varchar(80) CHARACTER SET ascii DEFAULT NULL,
firstName varchar(150) COLLATE utf8mb4_unicode_ci,
lastName varchar(150) COLLATE utf8mb4_unicode_ci,
email varchar(80) COLLATE utf8mb4_unicode_ci,
vanityURL varchar(255) COLLATE utf8mb4_unicode_ci,
interests json NOT NULL,
experience json NOT NULL,
countryCode varchar(255) CHARACTER SET ascii,
status varchar(25) CHARACTER SET  ascii,
reputationPoints int(11) DEFAULT '0',
createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

-- drop table if exists edx.EmailMemberSegmentationEdExSub;
-- create table edx.EmailMemberSegmentationEdExSub (
-- memberId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
-- email varchar(80) COLLATE utf8mb4_unicode_ci,
-- status varchar(25) CHARACTER SET  ascii,
-- class char(36) CHARACTER SET ascii DEFAULT '',
-- createdAt DATETIME
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- COMMIT;

drop table if exists edx.Course;
CREATE TABLE edx.Course (
  id char(36) CHARACTER SET ascii NOT NULL,
  vanityURL varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  siteID char(36) CHARACTER SET ascii NOT NULL,
  title varchar(150) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  shortDescription varchar(275) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description mediumtext COLLATE utf8mb4_unicode_ci NOT NULL,
  type char(36) NOT NULL,
  courseTypeID char(36) CHARACTER SET ascii NOT NULL,
  difficulty char(36) NOT NULL,
  assets json NOT NULL,
  settings json NOT NULL,
  tags json NOT NULL,
  theme varchar(50) CHARACTER SET utf8mb4 NOT NULL DEFAULT '',
  workshops json NOT NULL,
  contentStandards json NOT NULL,
  academicLevels json NOT NULL,
  products json NOT NULL,
  subjects json NOT NULL,
  badges json NOT NULL,
  forumID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  educators json NOT NULL,
  relatedContent json NOT NULL,
  status char(36) NOT NULL,
  enrollmentOpensAt datetime DEFAULT NULL,
  startsAt datetime DEFAULT NULL,
  enrollmentClosesAt datetime NOT NULL,
  publishAt datetime DEFAULT NULL,
  closesAt datetime DEFAULT NULL,
  forumClosesAt datetime DEFAULT NULL,
  graduationAt datetime DEFAULT NULL,
  createdAt datetime NOT NULL,
  createdBy char(36) CHARACTER SET ascii NOT NULL,
  updatedAt datetime DEFAULT NULL,
  updatedBy char(36) CHARACTER SET ascii DEFAULT NULL,
  requestID char(36) CHARACTER SET ascii DEFAULT NULL,
  credlyBadges json NOT NULL,
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.Course_hist;
CREATE TABLE edx.Course_hist (
  id char(36) CHARACTER SET ascii NOT NULL,
  vanityURL varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  siteID char(36) CHARACTER SET ascii NOT NULL,
  title varchar(150) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  shortDescription varchar(275) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  description mediumtext COLLATE utf8mb4_unicode_ci NOT NULL,
  type char(36) NOT NULL,
  courseTypeID char(36) CHARACTER SET ascii NOT NULL,
  difficulty char(36) NOT NULL,
  assets json NOT NULL,
  settings json NOT NULL,
  tags json NOT NULL,
  theme varchar(50) CHARACTER SET utf8mb4 NOT NULL DEFAULT '',
  workshops json NOT NULL,
  contentStandards json NOT NULL,
  academicLevels json NOT NULL,
  products json NOT NULL,
  subjects json NOT NULL,
  badges json NOT NULL,
  forumID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  educators json NOT NULL,
  relatedContent json NOT NULL,
  status char(36) NOT NULL,
  enrollmentOpensAt datetime DEFAULT NULL,
  startsAt datetime DEFAULT NULL,
  enrollmentClosesAt datetime NOT NULL,
  publishAt datetime DEFAULT NULL,
  closesAt datetime DEFAULT NULL,
  forumClosesAt datetime DEFAULT NULL,
  graduationAt datetime DEFAULT NULL,
  createdAt datetime NOT NULL,
  createdBy char(36) CHARACTER SET ascii NOT NULL,
  updatedAt datetime DEFAULT NULL,
  updatedBy char(36) CHARACTER SET ascii DEFAULT NULL,
  requestID char(36) CHARACTER SET ascii DEFAULT NULL,
  credlyBadges json NOT NULL,
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


drop table if exists edx.Resource;
CREATE TABLE edx.Resource (
  id char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  siteID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  title varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  shortDescription varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  description text COLLATE utf8mb4_unicode_ci NOT NULL,
  products json NOT NULL,
  subjects json NOT NULL,
  academicLevels json NOT NULL,
  tags json NOT NULL,
  internalTags json DEFAULT NULL,
  vanityURL varchar(255) CHARACTER SET ascii NOT NULL,
  SEOUrl varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  heroImage json NOT NULL,
  type char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  status enum('active','inactive','deleted') COLLATE utf8mb4_unicode_ci NOT NULL,
  subscribed bit(1) DEFAULT b'0',
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  createdBy char(36) CHARACTER SET ascii NOT NULL,
  updatedAt datetime DEFAULT NULL,
  updatedBy char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  publishedAt datetime DEFAULT NULL,
  requestID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  copyLicenses json NOT NULL,
  timing json DEFAULT NULL,
  ranking json DEFAULT NULL,
  technicalExpertise char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  standards json NOT NULL,
  links json NOT NULL,
  components json DEFAULT NULL,
  settings json DEFAULT NULL,
  public tinyint(1) NOT NULL DEFAULT '0',
  locked tinyint(1) NOT NULL DEFAULT '0',
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.Resource_hist;
CREATE TABLE edx.Resource_hist (
  id char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  siteID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  title varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  shortDescription varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  description text COLLATE utf8mb4_unicode_ci NOT NULL,
  products json NOT NULL,
  subjects json NOT NULL,
  academicLevels json NOT NULL,
  tags json NOT NULL,
  internalTags json DEFAULT NULL,
  vanityURL varchar(255) CHARACTER SET ascii NOT NULL,
  SEOUrl varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  heroImage json NOT NULL,
  type char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  status enum('active','inactive','deleted') COLLATE utf8mb4_unicode_ci NOT NULL,
  subscribed bit(1) DEFAULT b'0',
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  createdBy char(36) CHARACTER SET ascii NOT NULL,
  updatedAt datetime DEFAULT NULL,
  updatedBy char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  publishedAt datetime DEFAULT NULL,
  requestID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  copyLicenses json NOT NULL,
  timing json DEFAULT NULL,
  ranking json DEFAULT NULL,
  technicalExpertise char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  standards json NOT NULL,
  links json NOT NULL,
  components json DEFAULT NULL,
  settings json DEFAULT NULL,
  public tinyint(1) NOT NULL DEFAULT '0',
  locked tinyint(1) NOT NULL DEFAULT '0',
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


drop table if exists edx.Enrollment;
CREATE TABLE edx.Enrollment (
  memberID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  courseID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  status char(36) NOT NULL DEFAULT 'enrolled',
  reviewComment varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  progress int(11) DEFAULT '0',
  completedAt datetime DEFAULT NULL,
  startedAt datetime DEFAULT NULL,
  lastActivityAt datetime DEFAULT NULL,
  lastReviewedAt datetime DEFAULT NULL,
  lastReviewedBy char(36) CHARACTER SET ascii DEFAULT NULL,
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt datetime DEFAULT NULL,
  requestID char(36) CHARACTER SET ascii DEFAULT '',
  learningJournalURL varchar(512) CHARACTER SET ascii DEFAULT NULL,
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.Enrollment_hist;
CREATE TABLE edx.Enrollment_hist (
  memberID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  courseID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  status char(36) NOT NULL DEFAULT 'enrolled',
  reviewComment varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  progress int(11) DEFAULT '0',
  completedAt datetime DEFAULT NULL,
  startedAt datetime DEFAULT NULL,
  lastActivityAt datetime DEFAULT NULL,
  lastReviewedAt datetime DEFAULT NULL,
  lastReviewedBy char(36) CHARACTER SET ascii DEFAULT NULL,
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt datetime DEFAULT NULL,
  requestID char(36) CHARACTER SET ascii DEFAULT '',
  learningJournalURL varchar(512) CHARACTER SET ascii DEFAULT NULL,
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


drop table if exists edx.CourseToAcademicLevel;
create table edx.CourseToAcademicLevel (
courseId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
academicLevels char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.CourseToAcademicLevel_hist;
create table edx.CourseToAcademicLevel_hist (
courseId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
academicLevels char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.MemberSegmentation_temp;
create table edx.MemberSegmentation_temp (
memberId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
class_tmp char(36) CHARACTER SET ascii DEFAULT '',
class char(36) CHARACTER SET ascii DEFAULT '',
rule char(36) CHARACTER SET ascii DEFAULT '',
createdAt DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.MemberSegmentation_temp_backup;
create table edx.MemberSegmentation_temp_backup (
memberId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
class_tmp char(36) CHARACTER SET ascii DEFAULT '',
class char(36) CHARACTER SET ascii DEFAULT '',
rule char(36) CHARACTER SET ascii DEFAULT '',
createdAt DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists edx.MemberSegmentation;
create table edx.MemberSegmentation (
memberId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
classDesc char(120) CHARACTER SET ascii DEFAULT '',
class char(36) CHARACTER SET ascii DEFAULT '',
rule char(36) CHARACTER SET ascii DEFAULT '',
createdAt DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE edx.MemberSegmentation
ADD UNIQUE (memberId);
COMMIT;

drop table if exists edx.MemberSegmentation_Incr;
create table edx.MemberSegmentation_Incr (
memberId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
classDesc char(120) CHARACTER SET ascii DEFAULT '',
class char(36) CHARACTER SET ascii DEFAULT '',
rule char(36) CHARACTER SET ascii DEFAULT '',
createdAt DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

-- 19.12.2022
DROP TABLE IF EXISTS edx.StaticPage;
CREATE TABLE edx.StaticPage (
  id char(36) NOT NULL DEFAULT '',
  title varchar(255) NOT NULL DEFAULT '',
  description varchar(255) NOT NULL DEFAULT '',
  vanityURL varchar(500) NOT NULL DEFAULT '',
  html mediumtext NOT NULL,
  css mediumtext NOT NULL,
  categories json,
  isSecure tinyint(1) NOT NULL DEFAULT '0',
  isSearchable tinyint(1) NOT NULL DEFAULT '0',
  status enum('active','inactive','deleted') NULL,
  publishedAt datetime DEFAULT NULL,
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  createdBy char(36),
  updatedAt datetime DEFAULT NULL,
  updatedBy char(36) NOT NULL DEFAULT '',
  siteID char(36) NOT NULL DEFAULT '',
  requestID char(36) NOT NULL DEFAULT '',
  wordpressID int(11) DEFAULT NULL,
  glueID varchar(30) DEFAULT NULL,
  faqPage json DEFAULT NULL,
  products json DEFAULT NULL,
  subjects json DEFAULT NULL,
  academicLevels json DEFAULT NULL,
  isCollection tinyint(1) NOT NULL DEFAULT '0',
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


DROP TABLE IF EXISTS edx.StaticPage_hist;
CREATE TABLE edx.StaticPage_hist (
  id char(36) NOT NULL DEFAULT '',
  title varchar(255) NOT NULL DEFAULT '',
  description varchar(255) NOT NULL DEFAULT '',
  vanityURL varchar(500) NOT NULL DEFAULT '',
  html mediumtext NOT NULL,
  css mediumtext NOT NULL,
  categories json,
  isSecure tinyint(1) NOT NULL DEFAULT '0',
  isSearchable tinyint(1) NOT NULL DEFAULT '0',
  status enum('active','inactive','deleted') NULL,
  publishedAt datetime DEFAULT NULL,
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  createdBy char(36),
  updatedAt datetime DEFAULT NULL,
  updatedBy char(36) NOT NULL DEFAULT '',
  siteID char(36) NOT NULL DEFAULT '',
  requestID char(36) NOT NULL DEFAULT '',
  wordpressID int(11) DEFAULT NULL,
  glueID varchar(30) DEFAULT NULL,
  faqPage json DEFAULT NULL,
  products json DEFAULT NULL,
  subjects json DEFAULT NULL,
  academicLevels json DEFAULT NULL,
  isCollection tinyint(1) NOT NULL DEFAULT '0',
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- //19.12.2022

-- 24.01.2023
DROP TABLE IF EXISTS edx.school_domain;
CREATE TABLE edx.school_domain (
domain varchar(80),
school_type_desc varchar(40) DEFAULT NULL,
school_type varchar(40) DEFAULT NULL,
school_class varchar(40) DEFAULT NULL,
PRIMARY KEY (domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
-- //24.01.2023

-- 27.01.2023
DROP TABLE IF EXISTS edx.Rating;
CREATE TABLE edx.Rating (
  id char(36) CHARACTER SET ascii NOT NULL,
  rating int(11) NOT NULL DEFAULT 0,
  siteID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  entityID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  entityType enum('Resource','Comment','Course','Discussion','Workshop') CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  createdAt timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  createdBy char(36) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  updatedAt timestamp NULL DEFAULT NULL,
  updatedBy char(36) CHARACTER SET ascii DEFAULT '',
  requestID char(36) CHARACTER SET ascii DEFAULT '',
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS edx.Rating_hist;
CREATE TABLE edx.Rating_hist (
  id char(36) CHARACTER SET ascii NOT NULL,
  rating int(11) NOT NULL DEFAULT 0,
  siteID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  entityID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  entityType enum('Resource','Comment','Course','Discussion','Workshop') CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  createdAt timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  createdBy char(36) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  updatedAt timestamp NULL DEFAULT NULL,
  updatedBy char(36) CHARACTER SET ascii DEFAULT '',
  requestID char(36) CHARACTER SET ascii DEFAULT '',
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS edx.Favorite;
CREATE TABLE edx.Favorite (
  id char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  entityID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  entityType enum('Resource','Discussion','Course') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  status enum('active','inactive','deleted') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  createdBy char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  updatedAt datetime DEFAULT NULL,
  updatedBy char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  requestID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  siteID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS edx.Favorite_hist;
CREATE TABLE edx.Favorite_hist (
  id char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  entityID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  entityType enum('Resource','Discussion','Course') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  status enum('active','inactive','deleted') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  createdBy char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  updatedAt datetime DEFAULT NULL,
  updatedBy char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  requestID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  siteID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS edx.PlayList;
CREATE TABLE edx.PlayList (
  id char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  title char(36) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  memberID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  siteID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  status enum('active','deleted') COLLATE utf8mb4_unicode_ci NOT NULL,
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt datetime DEFAULT NULL,
  deletedAt datetime DEFAULT NULL,
  vanityURL varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  sortOrderCounter int(11) NOT NULL DEFAULT 0,
  activeItemsCount int(11) NOT NULL DEFAULT 0,
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS edx.PlayList_hist;
CREATE TABLE edx.PlayList_hist (
  id char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  title char(36) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  memberID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  siteID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  status enum('active','deleted') COLLATE utf8mb4_unicode_ci NOT NULL,
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt datetime DEFAULT NULL,
  deletedAt datetime DEFAULT NULL,
  vanityURL varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  sortOrderCounter int(11) NOT NULL DEFAULT 0,
  activeItemsCount int(11) NOT NULL DEFAULT 0,
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS edx.PlayListItem;
CREATE TABLE edx.PlayListItem (
  id char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  playlistID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  type char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  itemID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  status enum('active','deleted') COLLATE utf8mb4_unicode_ci NOT NULL,
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt datetime DEFAULT NULL,
  deletedAt datetime DEFAULT NULL,
  sortOrder int(11) NOT NULL DEFAULT 0,
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP TABLE IF EXISTS edx.PlayListItem_hist;
CREATE TABLE edx.PlayListItem_hist (
  id char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  playlistID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  type char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  itemID char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  status enum('active','deleted') COLLATE utf8mb4_unicode_ci NOT NULL,
  createdAt datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt datetime DEFAULT NULL,
  deletedAt datetime DEFAULT NULL,
  sortOrder int(11) NOT NULL DEFAULT 0,
  dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
  isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- //27.01.2023
-- //Edex Tables


-- Hadoop Tables
drop table if exists hdp.enterprise_dim_org_education;
CREATE TABLE hdp.enterprise_dim_org_education (
org_id char(50) CHARACTER SET ascii NOT NULL,
org_name text(500),
jem_org_type char(50),
renga_org_type char(50),
market_segment char(50),
country char(10),
org_domain char(50),
is_parent char(10),
is_root_org char(10),
market_subsegment char(50),
esm_status char(50),
class_ varchar(70) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists hdp.enterprise_dim_org_education_hist;
CREATE TABLE hdp.enterprise_dim_org_education_hist (
org_id char(50) CHARACTER SET ascii NOT NULL,
org_name text(500),
jem_org_type char(50),
renga_org_type char(50),
market_segment char(50),
country char(10),
org_domain char(50),
is_parent char(10),
is_root_org char(10),
market_subsegment char(50),
esm_status char(50),
class_ varchar(70) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


drop table if exists hdp.EntMemberLicenseDelegation;
CREATE TABLE hdp.EntMemberLicenseDelegation (
member_guid char(80) CHARACTER SET ascii NOT NULL DEFAULT '',
org_id varchar(80) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
delegation_status varchar(80) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists hdp.EntMemberLicenseDelegation_hist;
CREATE TABLE hdp.EntMemberLicenseDelegation_hist (
member_guid char(80) CHARACTER SET ascii NOT NULL DEFAULT '',
org_id varchar(80) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
delegation_status varchar(80) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


-- MemberGuidMap, Map tabela imsAuthId
drop table if exists hdp.MemberGuidMap;
create table hdp.MemberGuidMap (
memberId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
userGuid char(80) COLLATE utf8mb4_unicode_ci,
status varchar(25) CHARACTER SET  ascii,
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

drop table if exists hdp.MemberGuidMap_hist;
create table hdp.MemberGuidMap_hist (
memberId char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
userGuid char(80) COLLATE utf8mb4_unicode_ci,
status varchar(25) CHARACTER SET  ascii,
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


--09.11.2022
drop table if exists hdp.EdexMemberSegmentationExtract;
CREATE TABLE hdp.EdexMemberSegmentationExtract (
memberguid char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
adobeguid char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
member_created varchar(36),
business_group varchar(50),
target_group varchar(50),
email varchar(80),
country varchar(80),
state_province varchar(36),
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;


drop table if exists hdp.EdexMemberSegmentationExtract_hist;
CREATE TABLE hdp.EdexMemberSegmentationExtract_hist (
memberguid char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
adobeguid char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
member_created varchar(36),
business_group varchar(50),
target_group varchar(50),
email varchar(80),
country varchar(80),
state_province varchar(36),
dateFrom char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
dateTo char(36) CHARACTER SET ascii NOT NULL DEFAULT '',
isCurrent char(36)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;
-- //Hadoop Tables



-- Elastic Search Tables
-- 19.12.2022
drop table if exists els.agg_elasticsearchevents;
CREATE TABLE els.agg_elasticsearchevents (
memberId memberId char(36) CHARACTER SET ascii COLLATE ascii_general_ci,
event_index char(36) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT '',
event char(50) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT '',
event_date date DEFAULT NULL,
eventLevel varchar(100) DEFAULT NULL,
points bigint DEFAULT NULL,
entityType varchar(100) DEFAULT NULL,
entityID varchar(100) DEFAULT NULL,
rating double DEFAULT NULL,
resourceID varchar(100) DEFAULT NULL,
courseID varchar(100) DEFAULT NULL,
workshopID varchar(100) DEFAULT NULL,
sessionID varchar(300) DEFAULT NULL,
events varchar(10) DEFAULT NULL,
min_event_timestamp datetime DEFAULT NULL,
max_event_timestamp datetime DEFAULT NULL,
event_year int DEFAULT NULL,
event_count int DEFAULT NULL,
createdAt datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
COMMIT;

-- CREATE INDEX idx_event_date ON els.agg_elasticsearchevents (event_date);
-- CREATE INDEX idx_event ON els.agg_elasticsearchevents (event);
-- CREATE INDEX idx_memberId ON els.agg_elasticsearchevents (memberID);

-- //Elastic Search Tables
