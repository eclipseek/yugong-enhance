package com.taobao.yugong.common;

import org.junit.Test;

import com.taobao.yugong.common.alarm.AlarmMessage;
import com.taobao.yugong.common.alarm.MailAlarmService;

public class AlarmTest {

    @Test
    public void testEmail() {

        MailAlarmService alarm = new MailAlarmService();
        alarm.setEmailHost("smtp.163.com");
        alarm.setEmailUsername("eclipseek@163.com");
        alarm.setEmailPassword("z8y4q11l");
        alarm.start();

        AlarmMessage message = new AlarmMessage("this is ljh test; next line", "zhangyq9@asiainfo.com");
        for (int i = 0; i< 10; i++) {
            alarm.sendAlarm(message);
        }

    }
}
