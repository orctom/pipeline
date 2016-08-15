package com.orctom.pipeline;

import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.AbstractProcedure;
import com.orctom.pipeline.precedure.Outlet;

public class RoleC extends Outlet {

  @Override
  protected String getPredecessorRoleName() {
    return "roleB";
  }

  @Override
  protected void onMessage(Message message) {
    System.out.println("[C] " + message);
  }
}
