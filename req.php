<?php

$conn=new mysqli('localhost','root','Password.11');
$conn->query('use finance');

$myObj->status=0;
$myObj->result='';
switch ($_REQUEST['req']){
	case 'option_expiration':
		$sql='select distinct expiration from cboe_option where symbol="'.trim($_REQUEST['sym']).'" order by  expiration';
		$result=$conn->query($sql);
		$myObj->result=array();
		foreach ($result->fetch_all() as $r){
			$myObj->result[]=$r[0];
		}
		
		break;
	case 'option_tradedate':
		$sql='select distinct tradedate from cboe_option where symbol="'.trim($_REQUEST['sym']).'"  and expiration="'.$_REQUEST['expiration'].'" order by  tradedate';
		$result=$conn->query($sql);
		$myObj->result=array();
		foreach ($result->fetch_all() as $r){
			$myObj->result[]=$r[0];
		}
		
		break;
}
$myJSON = json_encode($myObj);
echo $myJSON;
?>
