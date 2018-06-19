
-- Create helper tables to assist with populating fact tables from data sources

-- mapping of demographic column names in the students table to codes in demographic_dim table;
-- note 'NumberAsianPacificIslander' appears to be a sum of both smaller categories, so we ignore it.
DROP TABLE IF EXISTS student_demographic_columns;
CREATE TABLE student_demographic_columns (name varchar(50), code varchar(1));
INSERT INTO student_demographic_columns (name, code) VALUES
('NumberAsian', 'A'),
('NumberPacificIslander', 'P'),
('NumberAmericanIndianorAlaskanNative', 'I'),
('NumberBlack', 'B'),
('NumberHispanic', 'H'),
('NumberWhite', 'W'),
('NumberTwoOrMoreRaces', '2'),
('NumberMales', 'M'),
('NumberFemales', 'F');

-- Create dimensional schema

DROP TABLE IF EXISTS demographic_fact;
DROP TABLE IF EXISTS school_totals_fact;
DROP TABLE IF EXISTS school_year_dim;
DROP TABLE IF EXISTS school_dim;
DROP TABLE IF EXISTS role_type_dim;
DROP TABLE IF EXISTS demographic_dim;

CREATE TABLE school_year_dim (
id int not null PRIMARY KEY AUTO_INCREMENT,
name varchar(50) not null,
abbreviated varchar(50) not null,
year_start int not null,
year_end int not null,
INDEX (name),
INDEX (abbreviated)
);

CREATE TABLE school_dim (
id int not null PRIMARY KEY AUTO_INCREMENT,
name varchar(50) not null,
school_code int,
district varchar(50) not null,
INDEX (name),
INDEX (school_code),
INDEX (district)
);

CREATE TABLE role_type_dim (
id int not null PRIMARY KEY AUTO_INCREMENT,
name varchar(50),
INDEX (name)
);

CREATE TABLE demographic_dim (
id int not null PRIMARY KEY AUTO_INCREMENT,
name varchar(50),
type varchar(50), -- e.g. race, gender
letter_code varchar(1),
INDEX (name),
INDEX (type),
INDEX (letter_code)
);

CREATE TABLE demographic_fact (
id int not null PRIMARY KEY AUTO_INCREMENT,
school_year_id int not null,
school_id int not null,
role_type_id int not null,
demographic_id int not null,
count int not null,
INDEX idx_school_year_id (school_year_id),
INDEX idx_school_id (school_id),
INDEX idx_role_type_id (role_type_id),
INDEX idx_demographic_id (demographic_id),
FOREIGN KEY (school_year_id) REFERENCES school_year_dim(id),
FOREIGN KEY (school_id) REFERENCES school_dim(id),
FOREIGN KEY (role_type_id) REFERENCES role_type_dim(id),
FOREIGN KEY (demographic_id) REFERENCES demographic_dim(id)
);

CREATE TABLE school_totals_fact (
id int not null PRIMARY KEY AUTO_INCREMENT,
school_year_id int,
school_id int,
total_students int,
total_teachers int,
UNIQUE KEY (school_year_id, school_id)
);

-- Populate dimensional data

INSERT INTO demographic_dim (name, type, letter_code) VALUES
('White', 'Race', 'W'),
('Black or African American', 'Race', 'B'),
('American Indian or Alaska Native', 'Race', 'I'),
('Asian', 'Race', 'A'),
('Native Hawaiian or Other Pacific Islander', 'Race', 'P'),
('Two or more races', 'Race', '2'),
('Hispanic', 'Hispanic', 'H'),
('Male', 'Gender', 'M'),
('Female', 'Gender', 'F');

INSERT INTO role_type_dim (name) VALUES
('Student'),
('Teacher');

INSERT INTO school_year_dim (name, abbreviated, year_start, year_end) VALUES
('2016-2017', '2016-17', '2016', '2017');

INSERT INTO school_dim (name, school_code, district)
SELECT SchoolName, SchoolCode, LEAName
FROM school_directory;

-- Populate fact table

-- 'teachers' subquery finds teachers, as identified by droot code;
-- records are grouped by name + cert + school to collapse
-- multiple assignment records  into a single teacher per school.
-- inner join to school_dim to filter out assignments in non-schools.
-- note that race is a multivalued field.
INSERT INTO demographic_fact (school_year_id, school_id, role_type_id, demographic_id, count)
SELECT school_year_dim.id, school_dim.id, role_type_dim.id, demographic_dim.id, 1 from
(SELECT SchoolYear, LastName, cert, bldgn, race
FROM personnel
WHERE droot >= 31 AND droot <= 34
GROUP BY SchoolYear, LastName, cert, bldgn, race)
AS teachers
INNER JOIN school_dim ON teachers.bldgn = school_dim.school_code
INNER JOIN demographic_dim ON INSTR(teachers.race, demographic_dim.letter_code)
JOIN school_year_dim ON school_year_dim.name = teachers.SchoolYear
JOIN role_type_dim on role_type_dim.name = 'Teacher';

-- do 'sex' column
INSERT INTO demographic_fact (school_year_id, school_id, role_type_id, demographic_id, count)
SELECT school_year_dim.id, school_dim.id, role_type_dim.id, demographic_dim.id, 1 from
(SELECT SchoolYear, LastName, cert, bldgn, sex
FROM personnel
WHERE droot >= 31 AND droot <= 34
group by SchoolYear, LastName, cert, bldgn, sex)
AS teachers
INNER JOIN school_dim ON teachers.bldgn = school_dim.school_code
INNER JOIN demographic_dim ON teachers.sex = demographic_dim.letter_code
JOIN school_year_dim ON school_year_dim.name = teachers.SchoolYear
JOIN role_type_dim on role_type_dim.name = 'Teacher';

-- do 'hispanic' column
INSERT INTO demographic_fact (school_year_id, school_id, role_type_id, demographic_id, count)
SELECT school_year_dim.id, school_dim.id, role_type_dim.id, demographic_dim.id, 1 from
(SELECT SchoolYear, LastName, cert, bldgn, hispanic
FROM personnel
WHERE droot >= 31 AND droot <= 34
group by SchoolYear, LastName, cert, bldgn, hispanic)
AS teachers
INNER JOIN school_dim ON teachers.bldgn = school_dim.school_code
INNER JOIN demographic_dim ON teachers.hispanic = 'Y' AND demographic_dim.letter_code = 'H'
JOIN school_year_dim ON school_year_dim.name = teachers.SchoolYear
JOIN role_type_dim on role_type_dim.name = 'Teacher';


DROP PROCEDURE IF EXISTS load_students_into_facts;

delimiter //

-- for the student file: MySQL lacks pivot functionality in MySQL to
-- deal with its column-per-demographic structure, so we loop over
-- each column containing a demographic count, executing an insert.
CREATE PROCEDURE load_students_into_facts()
BEGIN
  DECLARE n INT DEFAULT 0;
  DECLARE i INT DEFAULT 0;  

  SELECT COUNT(*) FROM student_demographic_columns INTO n;
  SET i = 0;
  WHILE i < n DO
    -- can't use variable names in queries, so we do this as a prepared statement
    SET @demographic_column_name := (SELECT name FROM student_demographic_columns LIMIT i, 1);
    SET @demographic_code := (SELECT code FROM student_demographic_columns LIMIT i, 1);
    SET @sql := CONCAT('INSERT INTO demographic_fact (school_year_id, school_id, role_type_id, demographic_id, count)
SELECT school_year_dim.id, school_dim.id, role_type_dim.id, demographic_dim.id, students2.inner_count from
(select BuildingNumber, SchoolYear, ', @demographic_column_name, ' AS inner_count
from students)
AS students2
inner join school_dim ON students2.BuildingNumber = school_dim.school_code
left join demographic_dim ON demographic_dim.letter_code = ''', @demographic_code, '''
join school_year_dim ON school_year_dim.abbreviated = students2.SchoolYear
join role_type_dim on role_type_dim.name = ''Student''');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    SET i = i + 1;
  END WHILE;
END//

CALL load_students_into_facts();

-- create teacher totals
INSERT INTO school_totals_fact (school_year_id, school_id, total_teachers)
SELECT school_year_dim.id, school_dim.id, teacher_count FROM
(
-- teacher count
SELECT SchoolYear, bldgn, count(*) as teacher_count FROM
  -- teachers per school per year
  (SELECT SchoolYear, LastName, cert, bldgn, count(*)
  FROM personnel
  WHERE droot >= 31 AND droot <= 34
  GROUP BY SchoolYear, LastName, cert, bldgn) teachers
GROUP BY SchoolYear, bldgn) t
INNER JOIN school_year_dim ON t.SchoolYear = school_year_dim.name
INNER JOIN school_dim ON t.bldgn = school_dim.school_code
ON DUPLICATE KEY UPDATE total_teachers = teacher_count;

-- create student totals
INSERT INTO school_totals_fact (school_year_id, school_id, total_students)
SELECT school_year_dim.id, school_dim.id, TotalEnrollment FROM students
INNER JOIN school_year_dim ON students.SchoolYear = school_year_dim.abbreviated
INNER JOIN school_dim ON students.BuildingNumber = school_dim.school_code 
ON DUPLICATE KEY UPDATE total_students = TotalEnrollment;

-- Create views to facilitate final reporting queries

DROP VIEW IF EXISTS demographics_by_school;
CREATE VIEW demographics_by_school AS
SELECT school_year_dim.name as school_year, school_id, school_dim.district, school_dim.name as school,
  role_type_dim.name as role, demographic_dim.type, demographic_dim.name as demographic, 
  SUM(count) as demographic_count
  FROM demographic_fact
  INNER JOIN school_year_dim ON school_year_dim.id = demographic_fact.school_year_id
  INNER JOIN school_dim ON school_dim.id = demographic_fact.school_id
  INNER JOIN role_type_dim ON role_type_dim.id = role_type_id
  INNER JOIN demographic_dim ON demographic_dim.id = demographic_id
  GROUP BY
  school_year_dim.name, school_dim.district, school_dim.name, role_type_dim.name, demographic_dim.type, demographic_dim.name;

DROP VIEW IF EXISTS demographics_by_district;
CREATE VIEW demographics_by_district AS
SELECT school_year_dim.name as school_year, school_dim.district,
  role_type_dim.name as role, demographic_dim.type, demographic_dim.name as demographic, 
  SUM(count) as demographic_count
  FROM demographic_fact
  INNER JOIN school_year_dim ON school_year_dim.id = demographic_fact.school_year_id
  INNER JOIN school_dim ON school_dim.id = demographic_fact.school_id
  INNER JOIN role_type_dim ON role_type_dim.id = role_type_id
  INNER JOIN demographic_dim ON demographic_dim.id = demographic_id
  GROUP BY
  school_year_dim.name, school_dim.district, role_type_dim.name, demographic_dim.type, demographic_dim.name;

DROP VIEW IF EXISTS school_counts;
CREATE VIEW school_counts AS
SELECT school_dim.id as school_id, school_dim.name, total_teachers, total_students
FROM school_totals_fact
INNER JOIN school_dim ON school_dim.id = school_totals_fact.school_id;

DROP VIEW IF EXISTS district_counts;
CREATE VIEW district_counts AS
SELECT school_dim.district, sum(total_teachers) as district_teachers, sum(total_students) as district_students
FROM school_totals_fact
INNER JOIN school_dim ON school_dim.id = school_totals_fact.school_id
GROUP BY school_dim.district;

-- note: there exist schools with the same name, so we join by school_dim.id
DROP VIEW IF EXISTS summary_demographics_by_school;
CREATE VIEW summary_demographics_by_school AS
SELECT demographics_by_school.school_year, demographics_by_school.district, school,
role, type, demographic, demographic_count,
ROUND(demographic_count /
CASE WHEN role = 'Teacher' THEN school_counts.total_teachers ELSE school_counts.total_students END, 2) AS percent,
CASE WHEN role = 'Teacher' THEN school_counts.total_teachers ELSE school_counts.total_students END AS total
FROM demographics_by_school
INNER JOIN school_counts
ON demographics_by_school.school_id = school_counts.school_id;

DROP VIEW IF EXISTS summary_demographics_by_district;
CREATE VIEW summary_demographics_by_district AS
SELECT demographics_by_district.school_year, demographics_by_district.district, role, type, demographic, demographic_count,
ROUND(demographic_count /
CASE WHEN role = 'Teacher' THEN district_counts.district_teachers ELSE district_counts.district_students END, 2) AS percent,
CASE WHEN role = 'Teacher' THEN district_counts.district_teachers ELSE district_counts.district_students END AS total
FROM demographics_by_district
INNER JOIN district_counts
ON demographics_by_district.district = district_counts.district;
