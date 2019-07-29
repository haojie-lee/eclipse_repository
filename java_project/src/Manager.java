import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.table.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
public class Manager extends JPanel implements ActionListener
{
	JSplitPane jsp=new JSplitPane(JSplitPane.VERTICAL_SPLIT);
	private JPanel jpt=new JPanel();
	String[] str1=new String[3];
	public static String mgNo;
	String sql;
	DataBase db;
	private JLabel[] jlArray=new JLabel[]
	{
		new JLabel("       管理员名"),
		new JLabel("       权        限"),
	    new JLabel("        密       码")   
	};
	private JTextField[] jtxtArray=new JTextField[]
	{
		new JTextField(),
		new JTextField(),
		new JTextField()
	};
	private JButton[] jbArray=new JButton[]
	{
		new JButton("添加管理员"),
		new JButton("删除管理员"),
		new JButton("修改信息"),
		new JButton("查找信息")
	};
	//创建标题
	Vector<String> head=new Vector<String>();
	{
		head.add("管理员名");
		head.add("权限");	
		head.add("密码");	
	}
	//在下部子窗口中设置表格
	Vector<Vector> data=new Vector<Vector>();
    //创建表格模型
    DefaultTableModel dtm=new DefaultTableModel(data,head);
    //创建Jtable对象
	JTable jt=new JTable(dtm);
	//将JTable封装到滚动窗格
	JScrollPane jspn=new JScrollPane(jt);
	public Manager(String mgNo)
	{
		this.setLayout(new GridLayout(1,1));
	    this.mgNo=mgNo;
		//设置面板的上部分为空布局管理器
		jpt.setLayout(null);
		//设置jspt中分割条的初始位置
		jsp.setDividerLocation(115);
		//设置分隔条的宽度
		jsp.setDividerSize(4);
		jsp.setTopComponent(jpt);
		jsp.setBottomComponent(jspn);
		for(int i=0;i<3;i++)
		{
			jpt.add(jlArray[i]);
			jpt.add(jtxtArray[i]);	
		    jlArray[i].setBounds(15+i*250,20,100,25);
		    jtxtArray[i].setBounds(115+i*250,20,150,25);
		    jtxtArray[i].addActionListener(this);
		}
		this.add(jsp);
		//设置下部子窗格
    	jsp.setBottomComponent(jspn);
    	//将JButton添加进jpt,设置监听器
		for(int i=0;i<4;i++)
		{
			jpt.add(jbArray[i]);
			jbArray[i].setBounds(140+120*i,70,100,25);
			jbArray[i].addActionListener(this);
		}
	    //设置窗体的大小位置及可见性
		this.setBounds(5,5,600,500);
		this.setVisible(true);
	}
	
	public void setFlag(boolean b)
	{
		jbArray[0].setEnabled(b);
		jbArray[1].setEnabled(b);		
	}
	
	public void actionPerformed(ActionEvent e){
		if(e.getSource()==jtxtArray[0]){jtxtArray[1].requestFocus();}//设置键盘易用性			
		if(e.getSource()==jtxtArray[1]){jtxtArray[2].requestFocus();}//设置键盘易用性	
		sql="select permitted from manager where mgNo='"+mgNo+"'";
		db=new DataBase();
		String string="";
		try{
			int p=0;
			if(string.equals("1")){   p++;
				String jtxt=jtxtArray[0].getText().trim();
				if(jtxt.equals("")){//当输入为空，提示
					JOptionPane.showMessageDialog(this,	"请输入管理员名！！！",
						        "消息",JOptionPane.INFORMATION_MESSAGE);
				    return;
				}
				if(e.getSource()==jbArray[0]){//执行添加操作
					this.insertManager();
				}
				if(e.getSource()==jbArray[1]){//执行删除操作
					this.deleteManager();
				}
				if(e.getSource()==jbArray[2]){//执行修改操作
					this.updateManager();
				}
				if(e.getSource()==jbArray[3]){//执行查询操作
					this.selectManager();
				}
			}
			if(p==0){ //说明该管理员是普通管理员，没有修改管理员信息的权限
				jtxtArray[0].requestDefaultFocus();
				String jtxt=jtxtArray[0].getText().trim();
				if(jtxt.equals("")){//管理员名输入为空，提示
					JOptionPane.showMessageDialog(this,	"请输入管理员名！！！",
						        "消息",JOptionPane.INFORMATION_MESSAGE);
				    return;
				}
				else if(jtxt.equals(mgNo)){//输入了管理员名
					if(e.getSource()==jbArray[3]){//执行manager1方法
						this.manager1();
					}
					if(e.getSource()==jbArray[2]){//执行修改操作
						String str[]=new String[3];
						int row = jt.getSelectedRow();
						if(row>=0){
							for(int i=0;i<3;i++){
								str[i]=jt.getValueAt(row,i).toString();
							}
							sql="update manager set password='"
							     +str[2]+"' where mgNo="+Integer.parseInt(str[0].trim());
							     
							db=new DataBase();
							int i=0;
							if(i==1){//
								JOptionPane.showMessageDialog(this,"修改成功！！",
								                  "消息!!",JOptionPane.INFORMATION_MESSAGE);
								return;											
							}
							else{//操作错误提示
								JOptionPane.showMessageDialog(this,"请重新操作！！",
	                                   "Warning!!",JOptionPane.INFORMATION_MESSAGE);
								return;											
							}
						}
						else{//操作错误提示
							JOptionPane.showMessageDialog(this,"请重新操作！！",
	                               "Warning!!",JOptionPane.INFORMATION_MESSAGE);
							return;								
						}	
					}
				}
				else{//提示普通管理员没有的权限
					JOptionPane.showMessageDialog(this,	"你不能查看别人的信息,请重新输入！",
				        "消息",JOptionPane.INFORMATION_MESSAGE);
				    return;					
				}
			}
		}
		catch(Exception ex){ex.printStackTrace();}
	}
	public void insertManager(){
		for(int i=0;i<3;i++){
		    str1[i]=jtxtArray[i].getText().trim();		
		}
		if(!str1[0].equals("")&&!str1[1].equals("")&&!str1[2].equals("")){
			if(!str1[0].matches("^\\d+$")){//判断管理员名的格式
				JOptionPane.showMessageDialog(this,	"管理员名只能为数字！！！",
				        "消息",JOptionPane.INFORMATION_MESSAGE);
				return;							
			}
			int temp = Integer.parseInt(str1[0]);			
			sql="insert into manager(mgNo,Permitted,password) values("
			          +temp+",'"+str1[1]+"','"+str1[2]+"')";//执行插入功能
			db=new DataBase();
			int j=0;
			if(j==0){//提示失败信息
				JOptionPane.showMessageDialog(this,	"插入失败！！！",
				        "消息",JOptionPane.INFORMATION_MESSAGE);
				return;				
			}
			Vector<String> v=new Vector<String>();
		    for(int i=0;i<=2;i++){
				v.add(str1[i]);//将每列添加到临时数组v
				if(i<3){jtxtArray[i].setText("");}	
		    }
		    data.add(v);
			dtm.setDataVector(data,head);//更新table	
			jt.updateUI();
			jt.repaint();
			return;
		}
		else{//输入为空时提示
			JOptionPane.showMessageDialog(this,	"学生信息不能为空！！！",
						        "消息",JOptionPane.INFORMATION_MESSAGE);
						        return;	
		}
	}
	public void deleteManager(){
		String mgNo=jtxtArray[0].getText().trim();
		if(mgNo.equals("")){//为空提示
			JOptionPane.showMessageDialog(this,	"学号不能为空！！！",
						        "消息",JOptionPane.INFORMATION_MESSAGE);
			return;			
		}
		sql="select permitted from manager where mgNo="+Integer.parseInt(mgNo);
		db=new DataBase();
		String str="";
		try{
			if(str.equals("1")){//输出提示信息
	        	JOptionPane.showMessageDialog(this,	"不能删除超级管理员信息记录！！",
							        "警告",JOptionPane.INFORMATION_MESSAGE);
	        	return;	
			}
			else{//执行删除
				sql="delete from manager where mgNo="+Integer.parseInt(mgNo);
		    	db=new DataBase();
	        	JOptionPane.showMessageDialog(this,	"成功删除该管理员信息！！",
							        "消息",JOptionPane.INFORMATION_MESSAGE);
			}
		}
	    catch(Exception e){e.printStackTrace();}	
	}
	public void updateManager(){
		String str[]=new String[3];
		int row = jt.getSelectedRow();
		if(row>=0){
			for(int i=0;i<3;i++){//得到所选内容
				str[i]=jt.getValueAt(row,i).toString();
			}
			sql="update manager set mgNo='"+str[0]+"',permitted='"+str[1]
			   +"',password='"+str[2]+"' where mgNo="+Integer.parseInt(str[0].trim());
			db=new DataBase();
			JOptionPane.showMessageDialog(this,"修改成功！！",
			                      "消息!!",JOptionPane.INFORMATION_MESSAGE);
			return;								
		}
		if(row==-1){//提示
			JOptionPane.showMessageDialog(this,
			"请点击'查找'按钮,在下部的表格里更改,再选中所改行,点击'修改信息'按钮！",
			                         "Warning!!",JOptionPane.INFORMATION_MESSAGE);
			return;
		}		
	}
	public void selectManager(){
		if(jtxtArray[0].getText().equals("")){//提示
			JOptionPane.showMessageDialog(this,"输入不能为空，请重新输入！！！",
			                              "信息",JOptionPane.INFORMATION_MESSAGE);
			return;
		}
		int q=Integer.parseInt(jtxtArray[0].getText().trim());
	   	sql="select * from manager where mgNo="+q;
	    db=new DataBase();
		this.table();   	
	}
    public void manager1(){
       	sql="select * from manager where mgNo="+Integer.parseInt(jtxtArray[0].getText().trim());
        db=new DataBase();
		this.table(); 	    	
    }
    public void table(){
    	try{
		     int k=0;
			 Vector<Vector> vtemp = new Vector<Vector>();
			 
			 if(k==0){//若manager表中没有该管理员号，则输出提示对话框
			 	 JOptionPane.showMessageDialog(this,"没有您要查找的内容",
				                 "消息",JOptionPane.INFORMATION_MESSAGE);
			 }
			dtm.setDataVector(vtemp,head);
			jt.updateUI();
			jt.repaint();				 	
		}
		catch(Exception e){e.printStackTrace();}	
    }
    public static void main(String[]args)
    {
    	new Manager(mgNo);
    }
}