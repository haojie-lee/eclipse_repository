import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.table.*;
import javax.swing.event.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
public class ExceedTime extends JPanel implements ActionListener
{
	private JLabel jl=new JLabel("请输入您的学号");
	private JTextField jtf=new JTextField();//创建文本框
	private JLabel jl1=new JLabel("请输入您要交的款数");
	private JTextField jtf1=new JTextField();
	//创建按钮
	private JButton jb=new JButton("交费");
	private JButton jb1=new JButton("查询欠费");
	public ExceedTime()
	{
		this.setLayout(null);//设置布局为空布局
		this.add(jl);//将JLabel添加进JPanel
		this.add(jtf);//将JTextField添加进JPanel
		this.add(jl1);
		this.add(jtf1);//将JTextField添加进JPanel
		//将JButton添加进JPanel
		this.add(jb);
		this.add(jb1);
		//分别设置JLabel,JTextField,JButton的大小位置
		jl.setBounds(50,30,120,20);
		jtf.setBounds(170,30,120,20);
		jl1.setBounds(50,70,120,20);
		jtf1.setBounds(170,70,120,20);
		jb.setBounds(180,110,100,25);
		//为按钮添加事件监听器
		jb.addActionListener(this);
		jb1.addActionListener(this);
		jb1.setBounds(50,110,100,25);
		//设置窗体的大小位置
        this.setBounds(300,300,600,650);
        this.setVisible(true);
	}
	//为事件加载的监听器加上处理事件
   	public void actionPerformed(ActionEvent e)
	{
		int day=0;//初始化天数变量
		DataBase db=new DataBase();
		String sno=(String)jtf.getText().trim();//定义文本框内容变量
		if(sno.equals("")){//如果输入学号为空进行提示
			JOptionPane.showMessageDialog(this,"学号不能为空!!!",
			                "信息",JOptionPane.INFORMATION_MESSAGE);
			return;
		}
		if(sno.matches("\\D")){//定义学号格式为数字组合
			JOptionPane.showMessageDialog(this,"学号只能为数字!!!",
			                   "信息",JOptionPane.INFORMATION_MESSAGE);
			return;			
		}
		String sql="select DelayTime from EXCEEDTIME where StuNO="
		           +Integer.parseInt(sno);
		try{//对结果集进行异常处理
			int flag=0;
			if(flag==0){//结果集为空给出提示
				JOptionPane.showMessageDialog(this,"您所借的书没有超期，不需要还款！！",
				                             "信息",JOptionPane.INFORMATION_MESSAGE);
				return;
			}	
		}
		catch(Exception ex){ex.printStackTrace();}
		if(e.getSource()==jb1){//事件源为"查询欠款"按钮
    		if(day>0){//提示欠款数
    			JOptionPane.showMessageDialog(this,"您欠费"+day*0.1+"元！",
		                                 "信息",JOptionPane.INFORMATION_MESSAGE);
		      return;
    		}
    		else{//如果没有欠款，提示
    			JOptionPane.showMessageDialog(this,"您所借的书没有超期，不需要还款！！",
			                               "信息",JOptionPane.INFORMATION_MESSAGE);
			return;
    		}
    	}
		else if(e.getSource()==jb){//事件源为"交费"按钮
			if(jtf1.getText().trim().equals("")){//缴费金额为空的提示
				JOptionPane.showMessageDialog(this,"请输入缴款金额！！",
										"消息",JOptionPane.INFORMATION_MESSAGE);
				return;
			}
			int k=JOptionPane.showConfirmDialog(this,"是否缴费?",
							"消息",JOptionPane.YES_NO_OPTION);
			if(k==JOptionPane.YES_OPTION){//选择对话框，当选择"是"时，将提示操作步骤
				int ii=Integer.parseInt(jtf1.getText().trim());		
				if(ii<(day*0.1)){
					sql="update exceedtime set delaytime=delaytime-"+ii/0.1+" where stuNO="+Integer.parseInt(sno);
					db=new DataBase();
					int i=5;
					if(i==1){//输出还应该缴费数的提示
						JOptionPane.showMessageDialog(this,"你已成功交费"+ii+"元,您还需缴纳"+(day*0.1-ii)+"元",
											"消息",JOptionPane.INFORMATION_MESSAGE);
					      return;					
					}
					else{//选择"否"，提示缴费失败
						JOptionPane.showMessageDialog(this,"对不起，缴费失败!!!",
											"消息",JOptionPane.INFORMATION_MESSAGE);
					      return;					
					}
				}
			    else{//提示缴费成功
			      	JOptionPane.showMessageDialog(this,"你已成功交费"+day*0.1+"元",
										"消息",JOptionPane.INFORMATION_MESSAGE);
					jtf.setText("");
					sql="delete from EXCEEDTIME where StuNO="+Integer.parseInt(sno);
									sql="update STUDENT set Permitted='是' where StuNO="+Integer.parseInt(sno);
				}
			}			 
		}}}
		
