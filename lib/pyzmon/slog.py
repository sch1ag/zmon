import syslog
import sys

curr_facility = None

def slog(msg="No log message provided", level=syslog.LOG_WARNING, facility=None):
    global curr_facility
    if facility and curr_facility and facility != curr_facility:
        syslog.closelog()
        curr_facility = None

    if not curr_facility:
        if facility:
            curr_facility = facility
        else: 
            curr_facility = syslog.LOG_DAEMON            
        syslog.openlog(logoption=syslog.LOG_PID, facility=curr_facility)
        
    syslog.syslog(level, msg)
    

def sfatal(msg="No error message provided", level=syslog.LOG_ERR, facility=None):
    slog(level=level, facility=facility, msg=msg)
    sys.exit(1)
    
if __name__ == '__main__':
    slog("This is a test daemon message")
    slog("This is a test mail message", facility=syslog.LOG_MAIL)
    sfatal("This is a test cron error message", facility=syslog.LOG_CRON)
