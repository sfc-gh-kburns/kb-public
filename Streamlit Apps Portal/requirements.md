
## How to adjust and run this example requirements document
When you are done, then share this into the context of the AI Agent.  
Also share any sample code or documentation that you want Cursor to copy paste solutions out of
Then run this AI Agent command:  
/Generate Curor Rules generate some rules using the requirements.md document I've provided
That will generate several rules file.  Alter each of them with the drop down boxes to be "Always" included
When you are done, delete this requirements.md document and alter your rules files going forward

## Cursor should ignore all the lines above when generating rules files.  

## Cursor behaviour
1. If I issue you a challenging task where there are several options or confusion in my request, then ask me questions and / or give me options before proceeding with any code changes
2. While we are starting a task, I will enjoy your creativity.  However, as we progress through a task in debugging and testing I do not want creativity.  I want minimal changes to fix problems.  
3. If I ask you to do something that is is in conflict with any of the rules files, then make the related change to the rules files so that I can review the change
4. Do not make any changes until you have 95% confidence that you know what to build.  Ask me follow up questions until you have that confidence.  

## high level design
This is a streamlit in snowflake app that is designed for a landing page and to launch other streamlit in snowflake applications from.  The idea is this will be a simple app for end users where they can see apps they have access to launch and it can launch from there.   This will be run in Streamlit in Snowflake.

There will be a few administrative features including Application Security Access based on Username or Snowflake role, and the ability to add/remove applications from the portal with a configuration screen (no code required).



## Architecture and Tech requirements:
This is built for Streamlit in Snowflake and can also be run locally on a laptop for development and testing.  

Limit python libraries to using those available on this anaconda channel:  
https://repo.anaconda.com/pkgs/snowflake/

The app will have access to a Snowflake database but default the app will not have access to external data.  When needed, you can assist the developer to set up external network connections, API keys, etc... to connect to external information


## Detailed design
1. The landing page for all users is the Portal.  This page will be a grid of applications with simple image previews of the applications.  It can be a big grid of images that are links to the Streamlit apps in Snowflake. 
You can think of this page as something simliar to an Okta portal where users see all the applications they can use.   
2. We need to create a Snowflake group called StreamlitPortalAdmins.   If the end user is a member of that group, they will be able to see an additional page called Portal Configuration.   Lets put the portal configuration code in a seperate python file.
2a. The Portal Configuration page will allow the admins to add/remove Streamlit apps to the portal.   It will also allow the admin to add/remove users or Snowflake roles to each Streamlit app that is availablie.   This should determine what each end user can see on the main landing page.

## Project Tasks - update for each major release
Your task as part of this project is to:
1. Create the pages mentioned above.
2. Create the StreamlitPortalAdmins group.   Add KBURNS to it to start. 
3. Create the Portal Configuration page.   This page should allow:
  a. Admins to add/remove streamlit in snowflake applications to the portal.    It should be a selection from all the streamlit apps in the Snowflake environment the users is logged into.   You can get the full list of streamlits in the account by running this command:  show streamlits in account;   The "title" column is what we will want to display.   The selection UX should be easy to use, and make it obvious which streamlits are already included in the portal.
  b. It would be great if we could auto-generate a picture representing each app.   But if this is not possible, please provide and upload option to store an image for each app that appears in teh portal.
  c. Admins should be able to add/remove both users and Snowflake Roles to the ability to see each streamlit application in the portal.   This should also be easy to use and easy to understand who has access already. 


## Documentation
1. You will create end user documentation so they know how to use the app.  This should appear in a single page within the app
2. Code should have a lot of comments to make it easy for humans to understand
3. The rules documents will act as the functional and technical designs and you will update these accordingly as we progress with the design and development of the app 

## Test Plan
1. Maintain a test plan according to what is designed and developed 

## Optional: Version Control Guidelines
Git branching strategy
Include guidelines for commit messages
Specify which files should be in .gitignore

## Optional: Security Requirements:
Add guidelines for handling sensitive data
Specify authentication requirements
Include guidelines for API key management

## Optional: Documentation Requirements:
Add requirements for code documentation
Specify requirements for user documentation
Include API documentation guidelines if applicable

