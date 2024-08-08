select -- t.fiscal_yr_and_per,
       t.fiscal_yr_and_per_desc as "Fiscal Month",
       t.school_district_name as "District",
       t.courseId as "Course Id",
       t.title as Title,
       t.pod,
       t.vsky as "V Sky",
       t.sky_scraper as "Sky Scrapers",
       t.top_100 as "Top 100",
       t.high_rise as "High Rise",
       t.top_200 as "Top 200",
       t.top_500 as "Top 500",
       t.ace_llc as "ACE/LLC Course",
       t.course_type as "Course Type",
       date(t.publish_date) as "Publish Date",
       t.difficulty as "Difficulty",
       t.vanityURL,
       t.status,
       -- t.total_views as "Total Views",
       -- t.guest_views as "Guest Views",
       -- t.percent_guest_views as "Percent of Views made by Guests",
       t.member_views as "Total Member Views (New and Repeat)", -- "Member Views",
       t.percent_member_views as "Percent of Views made by Unique Members", -- "Percent of Views made by Members",
       t.unique_daily_views as "Unique Member Daily Views", -- "Unique Daily Views",
       t.unique_member_views as "Unique Member/Visitor Views", -- "Unique Member Views",
       t.unique_downloads as "Unique Member Downloads", -- "Unique Downloads",
       t.enrolled_quantity as "Unique Total Memers Enrolled", -- "Total Enrolled",
       t.course_enrolled as "Unique Enrollment Only", -- "Enrollment Only",
       t.course_stars as "Unique Members That Only Started a Course", -- "Course Starts",
       t.graduation as "Unqiue Members that Graduated", -- "Graduation",
       t.course_removed_review_incomlete as "Unqiue Members that have Courses for Review or Incomlete courses", -- "For Review and Incomlete courses",
       t.rate_completion_started as "Rate of Completion of a Course by Unqiue Members that Started", -- "Rate of Completion by those that Started",
       t.link_to_express_and_cc as "Unique Users who clicked on Link to Express and CC use from courses", -- "Link to Express and CC use from courses",
       t.clicks_to_templates as "Unique users that Clicked on templates links from courses", -- "Clicks to templates from courses",
       t.clicks_to_teaching_resources as "Unique users that Clicked on teaching resources from courses", -- "Clicks to teaching resources from courses",
       t.member_sign_up_course_enrollment as "Unique Member that signed up to enroll in a course" -- "Member sign up to course enrollment relationship"
from edx.rpt_content_detail_course_fin t
where 1=1 -- t.isCurrent = 1
order by 1,2,3,4,5,6,7,8,9,10,11,12,13 desc;