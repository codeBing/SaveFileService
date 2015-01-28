using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace WindowsService2
{
    public partial class SaveService : ServiceBase
    {
        System.Timers.Timer timer1;
        static string path = ConfigurationManager.AppSettings["FileDirectory"];
        static string backupPath = ConfigurationManager.AppSettings["BackupDirectory"];
        static DBHelper db = new DBHelper();

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

        //protected override void OnStart(string[] args)
        //{
        //    using (System.IO.StreamWriter sw = new System.IO.StreamWriter("C:\\log.txt", true))
        //    {
        //        sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss ") + "Start.");
        //    }
        //}

        //protected override void OnStop()
        //{
        //    using (System.IO.StreamWriter sw = new System.IO.StreamWriter("C:\\log.txt", true))
        //    {
        //        sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss ") + "Stop.");
        //    }
        //}

        private void timer1_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            saveFile();
        }

        private void saveFile()
        {
            string[] strFiles = Directory.GetFiles(path);
            string[] spilt;
            string[] spiltData;
            string fileName;

            //遍历整个文件夹
            foreach (string strFile in strFiles)
            {
                //取得文件名
                fileName = Path.GetFileName(strFile);
                //分割文件名
                spilt = fileName.Split('_');
                //设备号，记录日期和阈值相同，但时间戳较新，更新对应数据
                if (isExist(spilt) == 1)
                {
                    //读取指定文件第一行
                    using (StreamReader sr = File.OpenText(strFile))
                    {
                        string s = "";
                        s = sr.ReadLine();
                        spiltData = s.Split(',');
                        updateData(spilt, spiltData);
                    }

                }
                //不存在相同数据，直接插入
                if (isExist(spilt) == 0)
                {
                    //读取指定文件第一行
                    using (StreamReader sr = File.OpenText(strFile))
                    {
                        string s = "";
                        s = sr.ReadLine();
                        spiltData = s.Split(',');
                        insertData(spiltData, spilt[2]);
                    }
                }
                //目标文件已存在，删除目标文件
                if (File.Exists(backupPath + "\\" + fileName))
                {
                    File.Delete(backupPath + "\\" + fileName);
                }
                //移动源文件到备份文件夹
                if (File.Exists(strFile) && !File.Exists(backupPath + "\\" + fileName))
                {
                    File.Move(strFile, backupPath + "\\" + fileName);
                }
            }
        }

        //查找是否存在设备号，记录日期和阈值相同的数据
        private int isExist(string[] fileName)
        {
            try
            {
                DbCommand cmd = db.GetSqlStringCommond("SELECT * FROM `data` WHERE deviceId=" + fileName[0] + " AND date =" + fileName[1] + " AND threshold=" + fileName[3]);
                DataTable result = db.ExecuteDataTable(cmd);
                //如果设备号，记录日期和阈值相同
                if (result.Rows.Count > 0)
                {
                    DataRow row = result.Rows[0];
                    //设备号，记录日期和阈值相同，但时间戳不一样，更新对应数据
                    if (string.Compare(row[3].ToString(), fileName[2]) < 0)
                    {
                        return 1;
                    }
                    return -1;
                }
                //不存在相同数据，直接插入
                else
                {
                    return 0;
                }
            }
            catch
            {
                return -1;
            }
        }

        //设备号，记录日期和阈值相同，但时间戳不一样，更新对应数据
        private bool updateData(string[] fileName, string[] data)
        {
            try
            {
                DbCommand update = db.GetSqlStringCommond("update data set timestamp=" + fileName[2] + ",value1=" + data[4] + ",value2=" + data[5] + ",value3=" + data[6] + ",value4=" + data[7] + ",value5=" + data[8] +
                                     ",value6=" + data[9] + ",value7=" + data[10] + ",value8=" + data[11] + ",value9=" + data[12] + ",value10=" + data[13] + ",value11=" + data[14] + ",value12=" + data[15] + ",value13=" + data[16] +
                                     ",value14=" + data[17] + ",value15=" + data[18] + ",value16=" + data[19] + ",value17=" + data[20] + ",value18=" + data[21] + ",value19=" + data[22] + ",value20=" + data[23] + ",value21=" + data[24] +
                                     ",value22=" + data[25] + ",value23=" + data[26] + ",value24=" + data[27] + " WHERE deviceId=" + fileName[0] + " AND date =" + fileName[1] + " AND threshold=" + fileName[3]);
                db.ExecuteNonQuery(update);
            }
            catch
            {
                return false;
            }
            return true;
        }

        //不存在相同数据，直接插入
        private bool insertData(string[] data, string timestamp)
        {
            try
            {
                DbCommand cmd = db.GetSqlStringCommond("insert into data values(null," + data[0] + "," +
                            data[1] + data[2] + data[3] + "," + timestamp + "," + data[28] + "," + data[4] + "," + data[5] + "," + data[6] + "," + data[7] + "," + data[8] +
                            "," + data[9] + "," + data[10] + "," + data[11] + "," + data[12] + "," + data[13] + "," + data[14] + "," + data[15] + "," + data[16] +
                            "," + data[17] + "," + data[18] + "," + data[19] + "," + data[20] + "," + data[21] + "," + data[22] + "," + data[23] + "," + data[24] +
                            "," + data[25] + "," + data[26] + "," + data[27] + ")");
                db.ExecuteNonQuery(cmd);
            }
            catch
            {
                return false;
            }
            return true;
        }

        private void LogMessage(string action)
        {
            string path = Directory.GetCurrentDirectory();
            using (System.IO.StreamWriter sw = new System.IO.StreamWriter("D:\\saveFileServiceLog.txt", true))
            {
                sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss ") + action);
            }
        }
    }
}
