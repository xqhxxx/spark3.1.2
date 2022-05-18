
val sum=0

.foreach（
num=>{
sum.add(num)
}
）

因为 普通的foreach方法 （闭包检测） 在executor计算 并不会返回结果

导致本地sum还是0


累加器：将executor的值返回到driver端
将分布式计算的结果 返回来
acc

自定义累加器



广播变量 
Bc
//一个executor中闭包   导致大量重复数据 

saprk的广播变量可以将闭包的数保存到executor的内存中
不可更改

