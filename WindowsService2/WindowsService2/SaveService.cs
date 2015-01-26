using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace WindowsService2
{
    public partial class SaveService : ServiceBase
    {
        System.Timers.Timer timer1;
        
        public SaveService()
        {
            InitializeComponent();
            timer1 = new System.Timers.Timer();
            timer1.Interval = 30000;
            timer1.Elapsed += new System.Timers.ElapsedEventHandler(timer1_Elapsed);
        }

        protected override void OnStart(string[] args)
        {
            this.timer1.Enabled = true;
            this.LogMessage("start");
        }

        protected override void OnStop()
        {

            this.timer1.Enabled = false;
            this.LogMessage("stop");
        }

        private void timer1_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            LogMessage("检查服务运行！");
        }

        private void LogMessage(string xMsg)
        {
            try
            {
                DBHelper db = new DBHelper();
                //这里向数据库中插入一条信息为 xMsg的记录，下边是我调用事先写好的Db类添加记录的方法，您也可以使用其他办法来写入数据库。
                //Db.QuerySQL("Insert into SysMsg (SysMsg) values ('"+xMsg+"')");
                DbCommand cmd = db.GetSqlStringCommond("insert into testservice values('" + xMsg + "')");
                db.ExecuteNonQuery(cmd);
            }
            catch
            {
                //不做任何操作
            }
        }
    }
}
