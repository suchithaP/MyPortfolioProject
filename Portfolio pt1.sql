use portfolioproject;
CREATE TABLE coviddeaths (iso_code varchar(1000),continent varchar(1000),location varchar(1000),date varchar (1000),
population int,total_cases int,new_cases int,new_cases_smoothed double,
total_deaths int, new_deaths int,new_deaths_smoothed double,
total_cases_per_million varchar(1000),new_cases_per_million text,new_cases_smoothed_per_million text,
total_deaths_per_million text,new_deaths_per_million text,new_deaths_smoothed_per_million text,
reproduction_rate text,icu_patients text,icu_patients_per_million text,hosp_patients text,
hosp_patients_per_million text,weekly_icu_admissions int,weekly_icu_admissions_per_million text,
weekly_hosp_admissions text,weekly_hosp_admissions_per_million text);
 
SELECT STR_TO_DATE(date,'%d-%m-%Y') as date FROM coviddeaths; #convert date in mysql date format
set SQL_SAFE_UPDATES = 0;
update coviddeaths set date = str_to_date(date, '%d-%m-%Y');  # ask primary key method to anyaa -------------
set SQL_SAFE_UPDATES = 1;
alter table coviddeaths modify date datetime;

LOAD DATA LOCAL INFILE 'C:/Users/admin/Downloads/CovidDeaths.csv'
INTO TABLE portfolioproject.coviddeaths FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';

select * from coviddeaths;
drop table coviddeaths;
explain coviddeaths;


CREATE TABLE covidvaccinations (iso_code varchar(1000),continent varchar(1000),location varchar(1000),date varchar (1000),
new_tests varchar(1000),total_tests varchar(1000),total_tests_per_thousand varchar(1000),new_tests_per_thousand varchar(1000),
new_tests_smoothed varchar(1000),new_tests_smoothed_per_thousand varchar(1000),
positive_rate text,tests_per_case text,tests_units text,
total_vaccinations text,people_vaccinated text,people_fully_vaccinated text,
new_vaccinations text,new_vaccinations_smoothed text,total_vaccinations_per_hundred text,people_vaccinated_per_hundred text,
people_fully_vaccinated_per_hundred text,new_vaccinations_smoothed_per_million text,stringency_index text,
population_density text,median_age text,aged_65_older text,aged_70_older text,gdp_per_capita text,extreme_poverty text,cardiovasc_death_rate text,
diabetes_prevalence text,female_smokers text,male_smokers text,handwashing_facilities text,hospital_beds_per_thousand text,
life_expectancy text,human_development_index text,excess_mortality text);
 
LOAD DATA LOCAL INFILE 'C:/Users/admin/Downloads/CovidVaccinations.csv'
INTO TABLE portfolioproject.covidvaccinations FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';

select * from covidvaccinations;
drop table covidvaccinations;

select * from coviddeaths where continent is not null order by 3,4; #to remove continent names in location column

select Location,date,total_cases,new_cases,total_deaths,population 
from coviddeaths order by 1,2;

-- Looking for Total Cases v/s Total Deaths --
-- Shows likelihood of dying if you contract covid in your country --
select location,date,total_cases,total_deaths,(total_deaths)/(total_cases)*100 as percent_deaths
from coviddeaths where location like '%indi%'
order by 1,2;

-- Looking at total cases v/s the population --
select location,date,population,total_cases,(total_cases)/(population)*100 as CasesPercent
from coviddeaths where location like '%indi%'
order by 1,2;

-- Countries with highest infection rate --
select location,population,max(total_cases) Infection_count,(max(total_cases))/(population)*100 as InfectionRate
from coviddeaths
group by location,population
order by InfectionRate desc;

set sql_safe_updates = 0;
UPDATE coviddeaths
SET continent = NULL 
WHERE continent = '';
set sql_safe_updates = 1;

-- Countries with highest death count vs the population --
select location,population,max(total_deaths) Deaths_count
from coviddeaths where continent is not null
group by location
order by Deaths_count desc;

-- Continents with highest death count vs the population --
select location,population,max(total_deaths) Deaths_count
from coviddeaths where continent is null  
group by location
order by Deaths_count desc;

select continent,population,max(total_deaths) Deaths_count
from coviddeaths where continent is not null  
group by continent
order by Deaths_count desc;

-- Global Numbers -- 
select date,sum(new_cases),sum(new_deaths),(sum(new_deaths))/(sum(new_cases))*100 as percent_deaths
from coviddeaths where continent is not null
group by date
order by 1,2;

select * from coviddeaths;
select * from covidvaccinations;

-- Join deaths and vaccinations tables -- 
select * from coviddeaths dea join covidvaccinations vac 
on dea.location = vac.location and
dea.date = vac.date;

SELECT STR_TO_DATE(date,'%d-%m-%Y') as date FROM covidvaccinations; #convert date in mysql date format
set SQL_SAFE_UPDATES = 0;
update covidvaccinations set date = str_to_date(date, '%d-%m-%Y');  # ask primary key method to anyaa -------------
set SQL_SAFE_UPDATES = 1;
alter table covidvaccinations modify date datetime;

-- Total vaccinated people v/s population --

select dea.continent,dea.location,dea.date,vac.new_vaccinations
from coviddeaths dea join covidvaccinations vac 
on dea.location = vac.location and
dea.date = vac.date where dea.continent is not null
order by 1,2;


WITH PopvsVac(continent,location,date,population,new_vaccinations,running_total_vaccinations)
as
(
SELECT dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations,sum(vac.new_vaccinations)
over (partition by dea.location order by dea.location , dea.date) as running_total_vaccinations -- (running_total_vaccinations)/population * 100
from coviddeaths dea join covidvaccinations vac 
on dea.location = vac.location and
dea.date = vac.date where dea.continent is not null
-- order by 2,3;
)
select *,(running_total_vaccinations/population)*100 from PopvsVac where location = 'India' ;

-- TEMP TABLE
Drop table if exists PercentPopulationVaccinated;
Create temporary Table PercentPopulationVaccinated (Continent varchar (255),location varchar (255), date datetime , population numeric,
new_vaccinations numeric,running_total_vaccinations numeric) ;

Drop table PercentPopulationVaccinated;
Insert into PercentPopulationVaccinated
SELECT dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations,sum(vac.new_vaccinations)
over (partition by dea.location order by dea.location , dea.date) as running_total_vaccinations -- (running_total_vaccinations)/population * 100
from coviddeaths dea join covidvaccinations vac 
on dea.location = vac.location and
dea.date = vac.date where dea.continent is not null;
-- order by 2,3
select *,(running_total_vaccinations/population)*100 from PercentPopulationVaccinated where location = 'India' ;

alter table covidvaccinations modify column new_vaccinations numeric; -- some data type error ask anyaa


-- Create view to store data for further visualisations
create view globalval 
as 
select date,sum(new_cases),sum(new_deaths),(sum(new_deaths))/(sum(new_cases))*100 as percent_deaths
from coviddeaths where continent is not null
group by date
order by 1,2;

create view PercentPopulationVaccinated1
as 
SELECT dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations,sum(vac.new_vaccinations)
over (partition by dea.location order by dea.location , dea.date) as running_total_vaccinations -- (running_total_vaccinations)/population * 100
from coviddeaths dea join covidvaccinations vac 
on dea.location = vac.location and
dea.date = vac.date where dea.continent is not null;
-- order by 2,3

select * from PercentPopulationVaccinated1;
select * from globalval;

SHOW FULL TABLES 
FROM portfolioproject
LIKE 'percent%';