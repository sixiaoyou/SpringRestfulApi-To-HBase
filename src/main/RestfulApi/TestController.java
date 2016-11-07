package controller;

import net.sf.json.JSONObject;

import java.util.Date;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Spring框架实现的Restful接口，处理PHP后台的Post请求
 * @author Administrator
 */

@Controller
@RequestMapping("/api")
public class TestController {
	HbaseDataOperation hdo ;

//json格式Post请求数据提交到/{projectName}/api/platformdata
	@RequestMapping(value = "/platformdata", method = RequestMethod.POST, produces = "application/json")
	@ResponseBody
	public void saveData(@RequestBody JSONObject platformdata) {
		if (hdo==null) {
			hdo=new HbaseDataOperation();
		}
//解析post请求中的数据，并执行相应地Hbase数据库操作，将操作状态结果封装成json返回给php后台程序。
		 hdo.compileJson(platformdata);
	}
}
