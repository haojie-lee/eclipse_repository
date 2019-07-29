import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.table.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
public class BorrowBook extends JPanel implements ActionListener{
	//创建分割方向为上下的JSplitePane对象
      private JSplitPane jsp1=new JSplitPane(JSplitPane.VERTICAL_SPLIT,true);
	private JPanel jp2=new JPanel();
	//创建按钮数组
	int flag;
	String sql;
	DataBase db;
	private JButton jb2=new JButton("确定");
	private JLabel jl3=new JLabel("您要借阅或预约的书号");
	private JLabel jl4=new JLabel("请输入您的学号");
	//在jsp1添加文本框
	private JTextField jtxt3=new JTextField();
	private JTextField jtxt4=new JTextField();
	//在jp2设置单选框
	private JRadioButton[] jrbArray=
    {new JRadioButton("借阅图书",true),new JRadioButton("预约图书")};
    private ButtonGroup bg=new ButtonGroup();
	Vector<String> head = new Vector<String>();	//创建标题
	{
		head.add("书号");
		head.add("书名");
		head.add("作者");
		head.add("出版社");
		head.add("是否借阅");
		head.add("是否预约");
	}	
	Vector<Vector> data=new Vector<Vector>();//表格数据向量集合    
    DefaultTableModel dtm=new DefaultTableModel(data,head);//创建表格模型   
	JTable jt=new JTable(dtm); //创建Jtable对象	
	JScrollPane jspn=new JScrollPane(jt);//将JTable放进滚动窗体
    public BorrowBook()
    {
    	this.setLayout(new GridLayout(1,1));
    	//把jsp2设置到jsp1的上部窗格
    	jsp1.setTopComponent(jp2);
    	//设置jsp1的下部窗格
    	jsp1.setBottomComponent(jspn);
    	//设置jsp1，jsp2中分割条的初始位置
    	jsp1.setDividerLocation(100);//设置分割控件位置
    	jsp1.setDividerSize(4);//设置分割控件宽度
    	jp2.setLayout(null);    	
		jb2.setBounds(380,20,100,20);//设置按钮的大小与位置
    	//将按钮添加进JPanel
		jp2.add(jb2);
		jb2.addActionListener(this);
		//设置JLabel的坐标
    	jl3.setBounds(80,60,130,20);
    	jl4.setBounds(330,60,100,20);
    	//把JLabel添加进JPanel
    	jp2.add(jl3);
    	jp2.add(jl4);	
    	jtxt3.setBounds(220,60,100,20);
    	jtxt4.setBounds(430,60,100,20);
    	jp2.add(jtxt3);
    	jp2.add(jtxt4);
    	for(int i=0;i<2;i++)
    	{
    		jrbArray[i].setBounds(70+i*150,20,150,20);
    		jp2.add(jrbArray[i]);
    		bg.add(jrbArray[i]);
    	}
    	this.add(jsp1);
    	//设置窗体的标题，大小位置及可见性
        this.setBounds(10,10,800,600);
        this.setVisible(true);  
    }	
    //为事件加载的监听器加上处理事件
	public void actionPerformed(ActionEvent e){
		if(e.getSource()==jb2){
			if(jtxt4.getText().equals("")){//为输入为空的情况进行处理
				JOptionPane.showMessageDialog(this,"输入不能为空，请重新输入！！！",
				                      "信息",JOptionPane.INFORMATION_MESSAGE);
				return;
			}
			//查询学号文本中所输学号是否存在于STUDENT表中
           	sql="select * from STUDENT where StuNO="+Integer.parseInt(jtxt4.getText().trim());
            db=new DataBase();
			Vector<Vector> vtemp = new Vector<Vector>();
			
				if(true){
					JOptionPane.showMessageDialog(this,"输入了错误的学号","消息",
					                              JOptionPane.INFORMATION_MESSAGE);
				}
				else{
										
						JOptionPane.showMessageDialog(this,"学号正确","消息",
	                              JOptionPane.INFORMATION_MESSAGE);
				}
		}}}
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			