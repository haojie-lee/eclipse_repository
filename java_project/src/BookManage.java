import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.table.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
public class BookManage extends JPanel implements ActionListener
{
	private JSplitPane jsp=new JSplitPane(JSplitPane.VERTICAL_SPLIT,true);
	private JPanel jpt=new JPanel();
	String []str1=new String [7];
	String sql; 
	DataBase db;
	private JLabel[] jlArray=new JLabel[]
	{
		new JLabel("         书    号"),
		new JLabel("         书    名"),
		new JLabel("         作    者"),
		new JLabel("         出 版 社"),
		new JLabel("         购买日期"),
		new JLabel("         已 预 约"),
		new JLabel("         已 借 阅")
	};
	private JTextField[] jtxtArray=new JTextField[]
	{
		new JTextField(),
		new JTextField(),
		new JTextField(),
		new JTextField(),
		new JTextField()
	};
	//设置JButton按钮的文本
	private JButton[] jbArray=
	{
	    new JButton("图书入库"),
	    new JButton("删除图书"),
	    new JButton("修改图书记录"),
	    new JButton("查找图书")
	};
	//创建标题
	Vector<String> head = new Vector<String>();
	{
		head.add("书号");
		head.add("书名");
		head.add("作者");
		head.add("出版社");
		head.add("购买日期");
		head.add("是否借阅");
		head.add("是否预约");
	}
	//在下部子窗口中设置表格
	Vector<Vector> data=new Vector<Vector>();
    //创建表格模型
    DefaultTableModel dtm=new DefaultTableModel(data,head);
    //创建Jtable对象
	JTable jt=new JTable(dtm);
	//将JTable封装到滚动窗格
	JScrollPane jspn=new JScrollPane(jt);
	//创建表示下拉列表框数据模型的字符串数组
	private String[] str={"否","是"};
	//创建下拉列表框
	private JComboBox jcp1=new JComboBox(str);
	private JComboBox jcp2=new JComboBox(str);
	public BookManage()
	{
		this.setLayout(new GridLayout(1,1));
		//设置面板的上部分为空布局管理器
		jpt.setLayout(null);
		//设置jspt中分割条的初始位置
		jsp.setDividerLocation(140);
		//设置分隔条的宽度
		jsp.setDividerSize(4);
		jsp.setTopComponent(jpt);
		jsp.setBottomComponent(jspn);
		for(int i=0;i<5;i++)
		{
			jpt.add(jtxtArray[i]);
		}
		for(int i=0;i<7;i++)
		{
			jpt.add(jlArray[i]);
			if(i<3)
			{
			    jlArray[i].setBounds(15,10+30*i,100,20);
			    jtxtArray[i].setBounds(115,10+30*i,150,20);
	
			}
			else if(i>2&&i<5)
			{
				jlArray[i].setBounds(265,10+30*(i-3),100,20);
				jtxtArray[i].setBounds(375,10+30*(i-3),120,20);
			}
			else
			{
				jlArray[i].setBounds(495,10+30*(i-5),100,20);	
			}	
		}
		for(int i=0;i<5;i++)
		{
			jtxtArray[i].addActionListener(this);
		}
		this.add(jsp);
		jpt.add(jcp1);
		jpt.add(jcp2);
		//设置下部子窗格
    	jsp.setBottomComponent(jspn);
		jcp1.setBounds(595,10,100,20);
		jcp2.setBounds(595,40,100,20);
		//将JButton添加进jpt
		for(int i=0;i<4;i++)
		{
			jpt.add(jbArray[i]);
			jbArray[i].setBounds(150+112*i,100,112,25);
		}
		//设置监听器
		for(int i=0;i<4;i++)
		{
			jbArray[i].addActionListener(this);
		}		
		//设置窗体的大小位置及可见性
		this.setBounds(5,5,600,500);
		this.setVisible(true);
	}
    public void actionPerformed(ActionEvent e){
    	//设置鼠标焦点
		if(e.getSource()==jtxtArray[0]){
			jtxtArray[1].requestFocus();
		}
    	if(e.getSource()==jtxtArray[1]){
    		jtxtArray[2].requestFocus();
    	}
    	if(e.getSource()==jtxtArray[2]){
    		jtxtArray[3].requestFocus();
    	}
    	if(e.getSource()==jtxtArray[3]){
    		jtxtArray[4].requestFocus();
    	}
		if(e.getSource()==jbArray[0]){//添加图书
			this.insertBook();
		} 
	    if(e.getSource()==jbArray[1]){//将书号为书号框的书从书库删除
	    	this.deleteBook();
	    	}	
	    if(e.getSource()==jbArray[2]){//将书号为书号框的书信息进行修改
	    	this.updateBook();
	    }	
	    if(e.getSource()==jbArray[3]){//查询图书信息
	    	this.searchBook();
	    }
	}
    public void insertBook(){
		for(int i=0;i<5;i++){//声明输入变量
            str1[i]=jtxtArray[i].getText().trim();		
		}
		if(str1[0].equals("")&&str1[1].equals("")&&str1[2].equals("")
			   &&str1[3].equals("")&&str1[4].equals("")){//当输入为空进行提示
			JOptionPane.showMessageDialog(this,	"图书信息不能为空！！！",
						        "消息",JOptionPane.INFORMATION_MESSAGE);
	            return;	
		}
	    if(!str1[0].equals("")&&!str1[1].equals("")&&!str1[2].equals("")
		   &&!str1[3].equals("")&&!str1[4].equals("")){//将图书信息插入Book表
			str1[5]=jcp1.getSelectedItem().toString();
			str1[6]=jcp2.getSelectedItem().toString();
			sql="insert into BOOK values('"+str1[0]+"','"+str1[1]+"','"
			 + str1[2] + "',' "+str1[3]+"','"+
			            str1[4]+"','"+str1[5]+"','"+str1[6]+"')";
			db=new DataBase();
			Vector<String> v = new Vector<String>();
		    for(int i=1;i<=7;i++){//将每列添加到临时数组v
				v.add(str1[i-1]);	
		    }
		    data.add(v);
			dtm.setDataVector(data,head);//更新table	
			jt.updateUI();
			jt.repaint();
			return;
		}
    }		
	public void deleteBook(){
		String bookno = jtxtArray[0].getText().trim();
		if(bookno.equals("")){//当书号输入为空，提示
			JOptionPane.showMessageDialog(this,	"书号不能为空！！！",
						        "消息",JOptionPane.INFORMATION_MESSAGE);
			return;			
		}
		sql="select * from RECORD where BookNO="+Integer.parseInt(bookno);
		db=new DataBase();
		sql="delete from book where BookNO="+Integer.parseInt(bookno);
	}
	public void updateBook(){
		String bookno = jtxtArray[0].getText().trim();
		if(bookno.equals("")){//当书号输入为空时，进行提示
			JOptionPane.showMessageDialog(this,	"请输入需要更改信息图书的书号！",
						               "消息",JOptionPane.INFORMATION_MESSAGE);
			return;			
		}
		else{//当输入书号后的情况
		    for(int i=0;i<5;i++){//声明文本框输入的变量
                str1[i]=jtxtArray[i].getText().trim();		
		    }
			db=new DataBase();
			int i=0;
			int flag=0;
			int b=Integer.parseInt(bookno);
			if(!str1[1].equals("")){i=i+1;}
			if(!str1[2].equals("")){i=i+2;}
			if(!str1[3].equals("")){i=i+4;}
			if(!str1[4].equals("")){i=i+8;}
			switch(i){//请详细解读switch语句
				case 0:
					JOptionPane.showMessageDialog(this,"信息不能为空！！！",
					                     "消息",JOptionPane.INFORMATION_MESSAGE);
				break;
				case 1:
				sql="update BOOK set BookName='"+str1[1]+"' where BookNO="+b;
				db=new DataBase();
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                       "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 2:
		        sql="update BOOK set Author='"+str1[2]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                     "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 3:
		        sql="update BOOK set BookName='"+str1[1]+"',"+"Author='"
		                      +str1[2]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                        "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 4:
		        sql="update BOOK set Publishment='"+str1[3]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                      "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 5:
		        sql="update BOOK set BookName='"+str1[1]+"',"+"Publishment='"
		                        +str1[3]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                     "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 6:
		        sql="update BOOK set Author='"+str1[2]+"',"+"Publishment='"
		                      +str1[3]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                       "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 7:
		        sql="update BOOK set BookName='"+str1[1]+"',"+"Author='"+str1[2]
		                       +"',"+"Publishment='"+str1[3]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                      "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 8:
		        sql="update BOOK set BuyTime='"+str1[4]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                        "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 9:
		        sql="update BOOK set BookName='"+str1[1]+"',"+"BuyTime='"
		                +str1[4]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                     "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 10:
		        sql="update BOOK set Author='"+str1[2]+"',"+"BuyTime='"
		                  +str1[4]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                      "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 11:
		        sql="update BOOK set BookName='"+str1[1]+"',"+"Author='"+str1[2]
		             +"',"+"BuyTime='"+str1[4]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                        "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 12:
		        sql="update BOOK set Publishment='"+str1[3]+"',"+"BuyTime='"
		                        +str1[4]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                       "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 13:
		        sql="update BOOK set BookName='"+str1[1]+"',"+"Publishment='"+str1[3]
		                       +"',"+"BuyTime='"+str1[4]+"' where BookNO="+b;
		       if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                      "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 14:
		        sql="update BOOK set Author='"+str1[1]+"',"+"Publishment='"+str1[2]
		                        +"',"+"BuyTime='"+str1[4]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                       "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
		        case 15:
		        sql="update BOOK set BookName='"+str1[1]+"',"+"Author='"+str1[2]+"',"+
		            "Publishment='"+str1[3]+"',"+"BuyTime='"+str1[4]+"' where BookNO="+b;
		        if(flag>0){
					JOptionPane.showMessageDialog(this,"恭喜你，修改成功!!!",
					                              "消息",JOptionPane.INFORMATION_MESSAGE);		        	
		        }break;
			}	
		}
	}
	public void searchBook(){//弹出提示对话框
		JOptionPane.showMessageDialog(this,	"请点击左边窗口的 '查询图书' 按钮！！",
						        "强烈推荐",JOptionPane.INFORMATION_MESSAGE);	
	    return;
	}	
}