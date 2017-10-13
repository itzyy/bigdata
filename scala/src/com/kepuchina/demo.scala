package com.kepuchina

/**
  * Created by Zouyy on 2017/9/26.
  */
class demo {

  def fab(n:Int):Int={
    if(n<=1)1
    else fab(n-1)+fab(n-2)
    }

  fab(3)
}
