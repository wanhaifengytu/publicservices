
from selenium import webdriver
from time import sleep
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.keys import Keys
import random

# you have 2 options for the job frequency:
    #1 for once
    #2 for daily
Frequency_choice = 1
start_date = "07/18/2019"
end_date = "08/11/2019"
start_hour = 11
start_min = 30
interval = 3
run_times = 1

# o for AM; 12 for PM
star_context = "0"

Company_list = ['CloneOfWalmartRBP','AHOLD','WMLoadH', 'PerfPepsiCo', 'SfPerfSiemens','SfPerfECTO','SiemensPEN','BPPerf3H']

#Company_list = ['PerfPepsiCo', 'SfPerfSiemens','SfPerfECTO','SiemensPEN']

report_list = ['notifyApprover','CheckToolExecution', 'timeSheetSubmit','initEffectiveLatestChangeInJobInfo','candidateMerge', 'initEffectiveLatestChangeInCompInfo', 'migrateGoalDataForAdhocReportBuilder','initEffectiveLatestChangeInJobInfo','candidateMerge', 'initEffectiveLatestChangeInCompInfo', 'migrateGoalDataForAdhocReportBuilder']

report_listAll = ['successlineNotificationJob','changeAuditToNoSQLSyncJob', 'notifyApprover', 'candidateMerge',
             'timeSheetSubmit', 'VPSysECTHistProcessor', 'refreshMasterJob', 'IRAsyncPredictJobType',
             'initEffectiveLatestChangeInJobInfo', 'initEffectiveLatestChangeInCompInfo', 'extToIntCandProfileConvert',
             'configSync', 'changeAuditCollectorJob', 'DMSGDPRInfoMigrationJobType', 'timeSheetGenerate',
             'migrateGoalDataForAdhocReportBuilder', 'hrisPCGSumsSync', 'candidateJobAlert', 'workflowActionReminder',
             'timeSheetSubmit', 'ExportDeltaToSFTPJobType', 'PositionRequisitionProcessing',
             'TLMTriggerRecalculation', 'CheckToolExecution', 'ecAlertsAndNotifications', 'ExportDeltaToSFTPJobType']

# report_list=['changeAuditToNoSQLSyncJob']


driver = webdriver.Chrome()
driver.get("http://10.116.28.44:8080/provisioning_login")

# login
driver.find_element_by_name("username").send_keys("SFV4")
driver.find_element_by_name("password").send_keys("sfv4")
driver.find_element_by_name("Login").click()



def create_Jobs():

    sleep(3)
    run_time = 0

    while run_time < run_times:
        count = 0
        print(count)
        while count < len(Company_list):

            RandomeNum = str(random.randint(0, 1000))
            print("random number is "+RandomeNum)
            try:
                for report in report_list:
                    #select company
                    print(Company_list[count])
                    driver.find_element_by_link_text(Company_list[count]).click()

                    sleep(2)

                    #go to "Manage Scheduled Jobs"
                    driver.find_element_by_link_text("Manage Scheduled Jobs").click()

                    sleep(2)
                    #go to "Create New Job"

                    driver.find_element_by_link_text("Create New Job").click()

                    sleep(5)
                    #locate the select
                    opt = driver.find_element_by_id("schedjob_jobtype")
                    #driver.find_element_by_id("schedjob_jobtype").click()
                    sleep(2)
                    #Select(opt).select_by_visible_text("Ad Hoc Reports Export")
                    Select(opt).select_by_value(report)
                    sleep(2)
                    #driver.find_element_by_name("sj_clij_compLibName").click()

                    #fulfill the job name
                    driver.find_element_by_name("schedjob_jobName").send_keys(Company_list[count]+"_"+report+RandomeNum)
                    # fulfill the job owner
                    print(Company_list[count]+"_"+report+RandomeNum)
                    elemt = driver.find_element_by_name("schedjob_jobOwner_text")
                    elemt.send_keys("Admin Admin")

                    sleep(5)
                    elemt.send_keys(Keys.ENTER)
                    if Frequency_choice == 2:
                        driver.find_element_by_xpath("//input[@name='schedjob_occurence' and @value='recurring']").click()

                        sleep(5)

                    # select time
                        jobtime = driver.find_element_by_name("schedjob_everyDailyHrs")
                        Select(jobtime).select_by_value("7")
                    # Select end date detail
                        driver.find_element_by_name("schedjob_endDate").send_keys(end_date)

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
                    #detail time:
                    #hour
                    start_hours = driver.find_element_by_name("schedjob_timeSlotStartHour")
                    #minus
                    start_mins = driver.find_element_by_name("schedjob_timeSlotStartMinute")
                    #am/ pm
                    start_contexts = driver.find_element_by_name("schedjob_timeSlotStartMeridiem")

                    Select(start_hours).select_by_value(str(start_hour))
                    Select(start_mins).select_by_value(str(start_min + (interval*run_time)))
                    print("the minus us "+str(start_min + (interval*run_time)))
                    Select(start_contexts).select_by_value(star_context)


                    #clcik on create button
                    driver.find_element_by_id("create_new").click()


                    #click submit

                    #driver.find_element_by_xpath("//a[@class='rollmenu rmbottomcxzcxzc']").click()
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
                print("Finish current company will work for next company")
                sleep(5)
                #driver.find_element_by_link_text('up to Company Listing')
            count = count + 1
            # if count < len(Company_list):
            #     print(Company_list[count] + " start!!")
            # else:
            #     break

        run_time = run_time + 1
        # print(run_time)
        # print(run_times)


    #driver.quit()
    print("complete!!")

        # except Exception:

        # else:
        #     driver.find_element_by_link_text("up to Company Listing")

create_Jobs()
