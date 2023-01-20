/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehcache.demos.peeper;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * @author Ludovic Orban
 */
@WebServlet(
  name = "PeeperServlet",
  urlPatterns = {"/*"}
)
public class PeeperServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    resp.setContentType("text/html");

    PrintWriter out = resp.getWriter();

    out.println("<html>");
    out.println("<head>");
    out.println("<title>Peeper</title>");
    out.println("</head>");
    out.println("<body>");
    out.println("<h1>Peeper!</h1>");

    try {
      List<String> allPeeps = PeeperServletContextListener.DATA_STORE.findAllPeeps();
      for (String peep : allPeeps) {
        out.println(peep);
        out.print("<br/><br/>");
      }
    } catch (Exception e) {
      throw new ServletException("Error listing peeps", e);
    }

    out.println("<form name=\"htmlform\" method=\"post\" action=\"peep\">");

    out.println("Enter your peep: <input type=\"text\" name=\"peep\" maxlength=\"142\" size=\"30\"/>");

    out.println("<input type=\"submit\" value=\"Peep!\"/>");
    out.println("</form>");
    out.println("</body>");
    out.println("</html>");
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException {
    String peepText = req.getParameter("peep");

    try {

      PeeperServletContextListener.DATA_STORE.addPeep(peepText);
      resp.sendRedirect("peep");

    } catch (Exception e) {
      throw new ServletException("Error saving peep", e);
    }
  }
}
