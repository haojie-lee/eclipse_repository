import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.table.*;
import javax.swing.event.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
public class SearchBook extends JPanel implements ActionListener{
	int flag;
	String sql;
	DataBase db;
   //创建分割方向为上下的JSplitePane对象
    private JSplitPane jsp=new JSplitPane(JSplitPane.VERTICAL_SPLIT,true);
	private JPanel jpt=new JPanel();//创建JPanel对象
	private JPanel jpb=new JPanel();
	//创建表示下拉列表框数据模型的字符串数组
	private String[] str={"书名","出版社","作者","购买时间"};
	private JComboBox jcb=new JComboBox(str);//创建下拉列表框
	private JButton jb=new JButton("提交");	//创建按钮
	private JLabel[] jlArray=new JLabel[]{
		new JLabel("       书    名"),
		new JLabel("     作    者"),
		new JLabel("出版社")
	};	
	private JTextField[] jtxtArray=new JTextField[]{//创建文本框
		new JTextField(),new JTextField(),
	    new JTextField(),new JTextField()
	};
	private JRadioButton[] jrbArray={//创建单选按钮
		new JRadioButton("简单查询",true),
		new JRadioButton("高级查询")
	};	
	private ButtonGroup bg=new ButtonGroup();//创建按钮组
	Vector<String> head = new Vector<String>();
	{//定义表头
		head.add("书号");head.add("书名");
		head.add("作者");head.add("出版社");
		head.add("购进时间");head.add("是否借阅");
		head.add("是否预约");
	}
	Vector<Vector> data=new Vector<Vector>();//定义检索出的书的基本信息
    DefaultTableModel dtm=new DefaultTableModel(data,head);	//创建表格模型
	JTable jt=new JTable(dtm); //创建Jtable对象
	JScrollPane jspn=new JScrollPane(jt);//将JTable封装到滚动窗格
	public SearchBook(){
		this.setLayout(new GridLayout(1,1));//设置查询图书界面为网格布局
		//设置整个RetrunBook界面上下部分均为空布局管理器
		jpt.setLayout(null);
		jpb.setLayout(null);
		//设置单选框的大小、位置，并添加事件监听器
		jpt.add(jcb);
		jcb.setBounds(160,20,150,20);	
	    jcb.addActionListener(this);
        //添加JButton设置其大小位置并添加事件监听器
		jpt.add(jb);
		jb.setBounds(560,20,120,20);
		jb.addActionListener(this);
		for(int i=0;i<2;i++){//对单选按钮进行设置
			jrbArray[i].setBounds(20,20+i*40,100,20);
			jpt.add(jrbArray[i]);
			jrbArray[i].addActionListener(this);
			bg.add(jrbArray[i]);
		}
		for(int i=0;i<3;i++){//设置标签和文本框的坐标，并将其添加进JPanel
			jlArray[i].setBounds(120+i*200,60,80,20);
			jtxtArray[i].setBounds(200+i*180,60,120,20);
			jpt.add(jtxtArray[i]);	
			jpt.add(jlArray[i]);
		}
		for(int i=0;i<3;i++){//设置文本框为不可用
			jtxtArray[i].setEditable(false);
		}
    	//设置文本框的坐标,并添加进jpt
		jtxtArray[3].setBounds(350,20,120,20);
		jpt.add(jtxtArray[3]);
        jsp.setTopComponent(jpt);//把jpt设置到jsp的上部窗格
        jsp.setBottomComponent(jspn);
        jsp.setDividerSize(4);
       	this.add(jsp);
    	jsp.setDividerLocation(100);//设置jsp中分割条的初始位置
		//设置窗体的大小位置及可见性
        this.setBounds(3,10,600,400);
        this.setVisible(true);
	}
    //为事件加载的监听器加上处理事件
	public void actionPerformed(ActionEvent e){
        if(jrbArray[0].isSelected()){//"简单查询"单选按钮被选中
        	jtxtArray[3].setEditable(true);
        	for(int i=0;i<jtxtArray.length-1;i++){//设置文本框为不可编辑
        		jtxtArray[i].setEditable(false);
        	}
        	if(jcb.getSelectedIndex()>=0&&jcb.getSelectedIndex()<4){
		    	jtxtArray[3].requestFocus();	    
			    if(e.getSource()==jb){//如果事件源为"提交"按钮，则执行检索
					String str=jtxtArray[3].getText().trim();
					if(str.equals("")){
						JOptionPane.showMessageDialog(this,"请输入必要的信息！！！",
										"消息",JOptionPane.INFORMATION_MESSAGE);
						return;
					}
				    if(jcb.getSelectedIndex()==0){//根据书名进行查询
						sql="select * from BOOK where BookName='"+str+"'";
			        	jtxtArray[3].setText("");
					}
					else if(jcb.getSelectedIndex()==1){//根据出版社进行查询
						sql="select * from BOOK where Publishment='"+str+"'";
						jtxtArray[3].setText("");
					}
					else if(jcb.getSelectedIndex()==2){//根据作者进行查询
						sql="select * from BOOK where Author='"+str+"'";
						jtxtArray[3].setText("");
					}
					else{//根据购进时间进行查询
						sql="select * from BOOK where BuyTime='"+str+"'";
						jtxtArray[3].setText("");
					}
					db=new DataBase();;
					//从表中检索成功后，把查到的书的所有信息显示在界面下部分的表中
					Vector<Vector> vtemp = new Vector<Vector>();
								
					dtm.setDataVector(vtemp,head);	//更新table	
					jt.updateUI();
					jt.repaint();								
				}
	        }
        }
		if(jrbArray[1].isSelected()){//"高级查询"单选按钮被选中
			 jtxtArray[0].requestFocus(); //获得输入焦点
			 jtxtArray[3].setEditable(false);
           	 for(int i=0;i<jtxtArray.length-1;i++){//将高级查询所涉及的文本框设为可编辑
        		jtxtArray[i].setEditable(true);
        	 }
			 if(e.getSource()==jb){//点击"提交"按钮
			 	int bz=this.seniorSearch();
			 	if(bz!=0){return;}
			 	db=new DataBase();
				//从表中检索成功后，把查到的书的所有信息显示在界面下部分的表中     
				Vector<Vector> vtemp = new Vector<Vector>();
				dtm.setDataVector(vtemp,head);//更新table
				jt.updateUI();
				jt.repaint();							
			 } 	
		}    
	}
	public int seniorSearch(){
		int flag=0;//设置标志位
		String str0=jtxtArray[0].getText().trim();
		String str1=jtxtArray[1].getText().trim();
		String str2=jtxtArray[2].getText().trim();
		if(str0.equals("")&&str1.equals("")&&str2.equals("")){//文本框输入为空
			JOptionPane.showMessageDialog(this,"请输入必要的信息！！！",
								"消息",JOptionPane.INFORMATION_MESSAGE);
			flag++;
		}
		if(((!str0.equals(""))&&(str1.equals(""))&&(str2.equals("")))
		     ||((str0.equals(""))&&(!str1.equals(""))&&(str2.equals("")))
		     ||((str0.equals(""))&&(str1.equals(""))&&(!str2.equals("")))){
			JOptionPane.showMessageDialog(this,"请使用简单查询！！！",
								"消息",JOptionPane.INFORMATION_MESSAGE);
			flag++;
		}
        if((!str0.equals(""))&&(!str1.equals(""))&&(str2.equals(""))){//书名和作者组合
			sql="select * from BOOK where BookName='"+str0+"' and Author='"+str1+"'";
			jtxtArray[0].setText("");jtxtArray[1].setText("");
		}
		if((!str0.equals(""))&&(str1.equals(""))&&(!str2.equals(""))){//书名和出版社组合
			sql="select * from Book where BookName='"+str0+"' and Publishment='"+str2+"'";
			jtxtArray[0].setText("");jtxtArray[2].setText("");
		}
		if((str0.equals(""))&&(!str1.equals(""))&&(!str2.equals(""))){//作者与出版社组合
			sql="select * from Book where Author='"+str1+"' and Publishment='"+str2+"'";
			jtxtArray[1].setText("");jtxtArray[2].setText("");
		}
		if((!str0.equals(""))&&(!str1.equals(""))&&(!str2.equals(""))){//三者组合
			sql="select * from Book where BookName='"+str0
						+"' and Publishment='"+str2+"' and Author='"+str1+"'";
			jtxtArray[0].setText("");jtxtArray[1].setText("");jtxtArray[2].setText("");
		}
		return flag;
	}
}