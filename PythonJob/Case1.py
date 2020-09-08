from selenium import webdriver
from time import sleep
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.keys import Keys
import random

# you have 2 options for the job frequency:
# 1 for once
# 2 for daily
Frequency_choice = 1
start_date = "07/19/2019"
end_date = "08/11/2019"
start_hour = 11
start_min = 55
interval = 3
run_times = 1

# o for AM; 12 for PM
star_context = "0"

Company_list = ['BPPerf3H', 'AHOLD', 'PerfPepsiCo','SfPerfSiemens','WMLoadH','SiemensPEN','SfPerfECTO','CloneOfWalmartRBP']

report_list = ['userExport', 'CustomerInstanceDataExport',
               'deltaUserExport', 'userExport', 'cMPMappingExport',
               'picklistExport', 'positionExport', 'jpbSkillExport', 'newHireExport', 'ProxyExptGeneral']


driver = webdriver.Chrome()
driver.get("http://10.116.28.44:8080/provisioning_login")

# login
driver.find_element_by_name("username").send_keys("SFV4")
driver.find_element_by_name("password").send_keys("sfv4")
driver.find_element_by_name("Login").click()


def create_NormalExport():
    sleep(5)
    run_time = 0

    while run_time < run_times:

        count = 0
        print(count)


        while count < len(Company_list):
            RandomeNum = str(random.randint(0, 1000))
            print("random number is " + RandomeNum)
            try:
                for report in report_list:
                    # select company
                    print(Company_list[count])
                    driver.find_element_by_link_text(Company_list[count]).click()

                    sleep(2)

                    # go to "Manage Scheduled Jobs"
                    driver.find_element_by_link_text("Manage Scheduled Jobs").click()

                    sleep(2)
                    # go to "Create New Job"

                    driver.find_element_by_link_text("Create New Job").click()

                    sleep(5)
                    # locate the select
                    opt = driver.find_element_by_id("schedjob_jobtype")
                    # driver.find_element_by_id("schedjob_jobtype").click()
                    sleep(2)
                    # Select(opt).select_by_visible_text("Ad Hoc Reports Export")
                    Select(opt).select_by_value(report)
                    sleep(2)
                    # driver.find_element_by_name("sj_clij_compLibName").click()

                    # fulfill the job name
                    driver.find_element_by_name("schedjob_jobName").send_keys(Company_list[count] + "_" + report + RandomeNum)
                    # fulfill the job owner
                    print(Company_list[count] + "_" + report + RandomeNum)
                    elemt = driver.find_element_by_name("schedjob_jobOwner_text")
                    elemt.send_keys("Admin Admin")

                    sleep(5)
                    elemt.send_keys(Keys.ENTER)

                    # fulfill the host address
                    sleep(3)
                    driver.find_element_by_name("hostAddress").send_keys("dc13sftp.lab-rot.saas.sap.corp")

                    driver.find_element_by_name("ftpLogin").send_keys("dc13-qa")
                    driver.find_element_by_name("ftpPwd").send_keys("kBGMUHBcS9")

                    driver.find_element_by_name("filePath").send_keys("/shan")

                    driver.find_element_by_name("fileName").send_keys(Company_list[count] + "_compLibExport")
                    # yyMMdd
                    dateformat = driver.find_element_by_name("dateSegment")
                    Select(dateformat).select_by_value("yyMMdd")
                   # driver.find_element_by_xpath("//*[@id=\"newJobform\"]/table[4]/tbody/tr[3]/td/input[2]").click()

                    if Frequency_choice == 2:
                        driver.find_element_by_xpath("//input[@name='schedjob_occurence' and @value='recurring']").click()

                        sleep(5)
                        # select time
                        jobtime = driver.find_element_by_name("schedjob_everyDailyHrs")
                        Select(jobtime).select_by_value("7")
                        # end date

                        driver.find_element_by_name("schedjob_endDate").send_keys(end_date)

                        # detail
                        end_hour = driver.find_element_by_name("schedjob_timeSlotEndHour")
                        # minus
                        end_mins = driver.find_element_by_name("schedjob_timeSlotEndMinute")
                        # am/ pm
                        end_time = driver.find_element_by_name("schedjob_timeSlotEndMeridiem")

                        Select(end_hour).select_by_value("10")
                        Select(end_mins).select_by_value("0")
                        Select(end_time).select_by_value("0")
                    elif Frequency_choice == 1:
                        print("just do this job for only once!")

                    driver.find_element_by_name("schedjob_startDate").send_keys(start_date)
                    # detail time:
                    # hour
                    start_hours = driver.find_element_by_name("schedjob_timeSlotStartHour")
                    # minus
                    start_mins = driver.find_element_by_name("schedjob_timeSlotStartMinute")
                    # am/ pm
                    start_time = driver.find_element_by_name("schedjob_timeSlotStartMeridiem")

                    Select(start_hours).select_by_value(str(start_hour))
                    Select(start_mins).select_by_value(str(start_min + (interval * run_time)))
                    print("the minus us " + str(start_min + (interval * run_time)))
                    Select(start_time).select_by_value(star_context)

                    # clcik on create button
                    driver.find_element_by_id("create_new").click()

                    # click submit

                    driver.find_element_by_xpath("//a[@class='rollmenu rmbottom']").click()

                    sleep(3)

                    driver.find_element_by_xpath("//a[contains(@id, 'submit')]").click()

                    sleep(2)
                    driver.switch_to_alert().accept()

                    sleep(3)
                    driver.find_element_by_link_text("up to Company Listing").click()
                    sleep(3)
                    print(Company_list[count] + " SUCCESS!")
            except Exception:
                pass
            else:
                driver.find_element_by_link_text('up to Company Listing')

            count = count + 1

        run_time = run_time + 1
    driver.quit()
    print("complete!!")

create_NormalExport()
