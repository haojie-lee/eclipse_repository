import java.awt.*;
import java.awt.event.*;
import javax.swing.event.*;
import javax.swing.*;
import javax.swing.tree.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import java.io.*;
import java.net.*;
public class StudentSystem extends JFrame implements ActionListener{
	DefaultMutableTreeNode[] dmtn={//创建节点数组
		new DefaultMutableTreeNode(new NodeValue("学生用户管理系统")),
	    new DefaultMutableTreeNode(new NodeValue("查询图书")),
	    new DefaultMutableTreeNode(new NodeValue("预约图书")),
	    new DefaultMutableTreeNode(new NodeValue("挂失图书")),
	    new DefaultMutableTreeNode(new NodeValue("退出"))
	};
    DefaultTreeModel dtm=new DefaultTreeModel(dmtn[0]);//创建树模型，指定根节点为"学生管理系统"
    JTree jt=new JTree(dtm);    //创建包含dtm树模型的JTree对象
    JScrollPane jsp=new JScrollPane(jt);    //为JTree创建滚动窗体    
    private JSplitPane jsplr=new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,true); //分割方向为上下方向
    Image image=new ImageIcon("tsgl.jpg").getImage();
    ImageIcon ii = new ImageIcon(image);    
    private JLabel jlRoot=new JLabel(ii);//标签文本自动居中
    private JPanel jp=new JPanel();    //创建JPanel对象
	CardLayout cl=new CardLayout();	//获取卡片布局管理器引用
	public StudentSystem(){
       	this.initJp();
    	jt.addTreeSelectionListener(new TreeSelectionListener(){//用内部类显示树的各选择节点
			public void valueChanged(TreeSelectionEvent e){
				DefaultMutableTreeNode cdmtn=
					(DefaultMutableTreeNode)e.getPath().getLastPathComponent();
				NodeValue cnv=(NodeValue)cdmtn.getUserObject();	
				if(cnv.value.equals("学生用户管理系统")){
					cl.show(jp,"root");//显示主界面
				}
				if(cnv.value.equals("查询图书")){													
					cl.show(jp,"ts");//显示"查询图书"界面
				}
				else if(cnv.value.equals("预约图书")){
					cl.show(jp,"OrderBook");//显示"预约图书"界面
				}
				else if(cnv.value.equals("挂失图书")){
					cl.show(jp,"GuaShi");//显示"归还挂失图书"界面
				}
				else if(cnv.value.equals("退出")){//用选择菜单提示是否退出系统
					int i=JOptionPane.showConfirmDialog(StudentSystem.this,
									"是否退出系统?","消息",JOptionPane.YES_NO_OPTION);
					if(i==JOptionPane.YES_OPTION){System.exit(0);}						
				}									
			}
		});
    	for(int i=1;i<dmtn.length;i++){//向根节点添加子节点
    		dtm.insertNodeInto(dmtn[i],dmtn[0],i-1);	
    	}
		jt.setEditable(false);//设置该树中节点是不可编辑的
		this.add(jsplr);//将包含树的滚动窗口添加进窗体
		jsplr.setLeftComponent(jt);//将包含树的滚动窗口添加进左边的子窗口
		//为jp设置大小位置并添加进右边的子窗口
		jp.setBounds(200,50,300,400);
		jsplr.setRightComponent(jp);
        jsplr.setDividerLocation(150);//设置分隔条的初始位置
        jsplr.setDividerSize(4); //设置分隔条的宽度
        jlRoot.setFont(new Font("Courier",Font.PLAIN,30));
		jlRoot.setHorizontalAlignment(JLabel.CENTER);
		jlRoot.setVerticalAlignment(JLabel.CENTER);
		//设置窗体的关闭动作，标题，大小，位置及可见性
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		//对标题和logo图片进行初始化	
 		Image image=new ImageIcon("ico.gif").getImage();
 		this.setIconImage(image);
		this.setTitle("学生管理系统");
		//设置窗体首次出现的大小和位置--自动居中
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
		int centerX=screenSize.width/2;
		int centerY=screenSize.height/2;
		int w=500;//本窗体宽度
		int h=400;//本窗体高度
		this.setBounds(centerX-w/2,centerY-h/2-100,w,h);//设置窗体出现在屏幕中央
		this.setExtendedState(JFrame.MAXIMIZED_BOTH);//窗体全屏
		this.setVisible(true);
		jt.setShowsRootHandles(true);//设置显示根节点的控制图标
    }
    public void initJp(){//对所引用的方法进行声明
    	jp.setLayout(cl);
    	jp.add(jlRoot,"root");
    	jp.add(new SearchBook(),"ts");
    	jp.add(new OrderBook(),"OrderBook");
    	jp.add(new GuaShi(),"GuaShi");
    }
    public void actionPerformed(ActionEvent e){}//实现ActionListener方法
	public static void main(String[]args){new StudentSystem();}
}
class NodeValue{//声明NodeValue类
	String value;
	public NodeValue(String value){
		this.value=value;
	}
	public String getValue(){//声明NodeValue构造器
		return this.value;
	}
	public String toString(){
		return value;
	}
}