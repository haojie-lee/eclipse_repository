import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.table.*;
import javax.swing.event.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
public class GuaShi extends JPanel implements ActionListener
{
	DataBase db;
	String sql;
	String str;
	//创建分割方向为上下的JSplitePane对象
    private JSplitPane jsp=new JSplitPane(JSplitPane.VERTICAL_SPLIT,true);
    //创建JPanel对象
	private JPanel jpt=new JPanel();
	private JPanel jpb=new JPanel();
	//创建按钮数组
	private JButton[] jbArray=new JButton[]
	{
		new JButton	("挂失"),
		new JButton	("确定")
	};
	private JLabel jl=new JLabel("请输入你的学号:");

	private JTextField jtxt=new JTextField();

	//创建标题
	Vector<String> head = new Vector<String>();
	{
		head.add("书号");
		head.add("学号");
		head.add("借阅时间");
		head.add("还书时间");
		head.add("是否过期");
		head.add("是否预约");
	}
	//
	Vector<Vector> data=new Vector<Vector>();
	//创建表格模型
    DefaultTableModel dtm=new DefaultTableModel(data,head);
    //创建Jtable对象
	JTable jt=new JTable(dtm);
	//将JTable封装到滚动窗格
	JScrollPane jspn=new JScrollPane(jt);
	public GuaShi()
	{
		this.setLayout(new GridLayout(1,1));
		//设置整个RetrunBook界面上下部分均为空布局管理器
		jpt.setLayout(null);
		jpb.setLayout(null);
		//设置Label的大小及位置
           jl.setBounds(5,15,100,20);	
		//将Jlabel添加到jpt面板上
		 jpt.add(jl);
		//为JTextField设置大小及位置
		jtxt.setBounds(105,15,300,20);
    	//把JTextField添加到jpt
	 jpt.add(jtxt);
        //设置JBuuton的大小及位置
	    jbArray[0].setBounds(5,50,100,20);
        jbArray[1].setBounds(150,50,100,20);
        //添加JButton并给其添加事件监听器
         for(int i=0;i<jbArray.length;i++)
		{
			 jpt.add(jbArray[i]);
			 jbArray[i].addActionListener(this);
		}
		//把jpt设置到jsp的上部窗格
    	jsp.setTopComponent(jpt);
        jsp.setBottomComponent(jspn);
    	jsp.setDividerSize(4);
    	this.add(jsp);
    	//设置jsp中分割条的初始位置
    	jsp.setDividerLocation(80);
		//设置窗体的大小位置及可见性
        this.setBounds(10,10,800,600);
        this.setVisible(true); 
   	} 
   	public void actionPerformed(ActionEvent e)
   	{
   		if(e.getSource()==jbArray[1]){//事件源为"确定"按钮
   			if(jtxt.getText().trim().equals("")){//学号输入为空，提示
   				JOptionPane.showMessageDialog(this,"请输入学号",
					            "消息",JOptionPane.INFORMATION_MESSAGE);
					return;
   			}
   			else{//根据学号进行查询
   				sql="select * from RECORD where StuNO="+jtxt.getText().trim();
	   			db=new DataBase();
	   			Vector<Vector> vtemp = new Vector<Vector>();
				try{//结果集进行异常处理
	                int k=0;
					
				    if(k==0){//提示
						JOptionPane.showMessageDialog(this,"输入了错误的学号或该学生没有借书记录",
						                             "消息",JOptionPane.INFORMATION_MESSAGE);
						return;
					}			
	   			}
	   			catch(Exception ea){ea.printStackTrace();}
			}
   		}
   		if(e.getSource()==jbArray[0]){//需要挂失图书
			int row=jt.getSelectedRow();
			if(row<0){
				JOptionPane.showMessageDialog(this,"请选择要挂失的书!!!","消息",
					                              JOptionPane.INFORMATION_MESSAGE);
				return;
			}
   			loseBook(row);
   			updateTable();
   		}
   	}
   		
   	public void loseBook(int row){
		String bname="";
		int lbno=0;
		int bno=Integer.parseInt((String)jt.getValueAt(row,0));//得到丢失书的书号
		String sno=(String)jt.getValueAt(row,1);//得到丢失书的人学号
		sql="select BookName from BOOK where BookNO="+bno;
		db=new DataBase();
		
		//找到最大的丢失记录号
		sql="select MAX(LbNO) from LoseBook";
		
		sql="insert into LOSEBOOK values("+lbno+","+sno+","+bno+",'"+bname+"')";//向丢书记录表中插入记录
		sql="select BookNo from ORDERREPORT where BookNO="+bno;//检查预约表中是否预约该书，若有，删除
		sql="select BookNo from EXCEEDTIME where BookNO="+bno;//检查超期表中是否有该书，若有，删除
		sql="delete from RECORD where BookNO="+bno;//从借书表中将丢失图书的记录删除
		sql="delete from BooK where BookNo="+bno;//从图书表中将丢失的书删除
		int i=0;
		if(i>0){//提示挂失成功
			JOptionPane.showMessageDialog(this,"恭喜你，挂失成功!!!","消息",
				                           JOptionPane.INFORMATION_MESSAGE);
			return;
		}
		else{//提示挂失失败
			JOptionPane.showMessageDialog(this,"对不起，挂失失败!!!",
					   	"信息",JOptionPane.INFORMATION_MESSAGE);
			return;
		}
   	}	
   	public void updateTable(){//实现界面下部表格的更新
		sql="select * from RECORD where StuNO="+jtxt.getText().trim();
		db=new DataBase();
		Vector<Vector> vtemp = new Vector<Vector>();
		
		jt.clearSelection();			
		dtm.setDataVector(vtemp,head);//更新table	
		jt.updateUI();
		jt.repaint();   		
   	}	
   	public int checkTime(int sno,int bno)
   	{//-1 代表超期没交罚款  0代表当天借的书   1代表正常还的书  -2表示超期交罚款
   		int day=0;
   		int flag=0;
   		String bname="";
   		Date now=new Date();
   		String returntime="";
   		sql="select ReturnTime from RECORD where StuNO="+sno+" and BookNO="+bno;
   		db=new DataBase();
   		
   		String[] strday=returntime.split("\\.");//这里使用了简单的正则式，规定了时间的格式
		int ryear=Integer.parseInt(strday[0].trim());
		int rmonth=Integer.parseInt(strday[1].trim());
		int rday=Integer.parseInt(strday[2].trim());
		day=(now.getYear()+1900-ryear)*365+(now.getMonth()+1-rmonth)*30+(now.getDate()-rday);
		if(day==-30)
		{//表示当天借的书
			JOptionPane.showMessageDialog(this,"今天借的书不能还！！！",
								"消息",JOptionPane.INFORMATION_MESSAGE);
			flag=0;
		}
		else if(day>0)
		{//代表超期了
			int i=JOptionPane.showConfirmDialog(this,"该书已过期,应交罚款为"
			             +day*0.1+"元,是否缴纳罚款?","消息",JOptionPane.YES_NO_OPTION);
			if(i==JOptionPane.YES_OPTION){
				JOptionPane.showMessageDialog(this,"你已成功交费"+day*0.1+"元",
										"消息",JOptionPane.INFORMATION_MESSAGE);
				flag=-2;
			}
			else
			{//超期且没交费
				flag=-1;
				sql="select BookName from BOOK where BookNO="+bno;
				sql="insert into EXCEEDTIME(StuNO,BookNO,BookName,DelayTime) values("+sno+","+bno+",'"+bname+"',"+day+")";
			}
		}
		else
		{//表示可以正常归还的书
			flag=1;
		}
		return flag;
   	}
}