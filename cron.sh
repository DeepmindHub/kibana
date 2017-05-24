# if [ $(ps -ef | grep 'rider_location.py'| wc -l) -lt 2 ]
# then
#   (python /home/ubuntu/datareports/kibana/rider_location.py >> /home/ubuntu/datareports/kibana/kibana.log 2>&1 &)
# fi
# if [ $(ps -ef | grep 'delivery_service.py'| wc -l) -lt 2 ]
# then
#   (python /home/ubuntu/datareports/kibana/delivery_service.py >> /home/ubuntu/datareports/kibana/kibana.log 2>&1 &)
# fi
if [ $(ps -ef | grep 'rider_attendance.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/datareports/kibana/rider_attendance.py >> /home/ubuntu/datareports/kibana/kibana.log 2>&1 &)
fi
# if [ $(ps -ef | grep 'otp_data.py'| wc -l) -lt 2 ]
# then
#   (python /home/ubuntu/datareports/kibana/otp_data.py >> /home/ubuntu/datareports/kibana/kibana.log 2>&1 &)
# fi
if [ $(ps -ef | grep 'hyperlocal_v4.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/datareports/kibana/hyperlocal_v4.py >> /home/ubuntu/datareports/kibana/kibana.log 2>&1 &)
fi
if [ $(ps -ef | grep 'ecom.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/datareports/kibana/ecom.py >> /home/ubuntu/datareports/kibana/kibana.log 2>&1 &)
fi
if [ $(ps -ef | grep 'ticket.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/datareports/kibana/ticket.py >> /home/ubuntu/datareports/kibana/kibana.log 2>&1 &)
fi
if [ $(ps -ef | grep 'clean_order.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/datareports/kibana/clean_order.py >> /home/ubuntu/datareports/kibana/kibana.log 2>&1 &)
fi
# if [ $(ps -ef | grep 'low_perform.py'| wc -l) -lt 2 ]
# then
#   (python /home/ubuntu/datareports/kibana/low_perform.py >> /home/ubuntu/datareports/kibana/kibana.log 2>&1 &)
# fi
if [ $(ps -ef | grep 'bb.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/datareports/kibana/bb.py >> /home/ubuntu/datareports/kibana/kibana.log 2>&1 &)
fi
if [ $(ps -ef | grep 'kibana_report.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/datareports/auto_allocation/kibana_report.py >> /home/ubuntu/datareports/auto_allocation/kibana_report.log 2>&1 &)
fi
if [ $(ps -ef | grep 'clean_order_v2.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/datareports/kibana/clean_order_v2.py >> /home/ubuntu/datareports/kibana/logs/clean_order_v2.log 2>&1 &)
fi
