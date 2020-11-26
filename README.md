# [README.md](http://readme.md/)

**/***

**Author : IDH**

**Date : 2020-04-30(last-updated)**

***/**

## **[ Installation Procedure ]**

   - **Install `Visual Studio Code`**

     Download link : https://code.visualstudio.com/download

   - Install Default plugin

     - Korean Language Pack for Visual Stduio Code
     - vscode-icons(Icon theme)
     - One Dark Pro (Color theme)
     - Bracket Pair Colorizer
     - Color Manager
     - select highlight in minimap
     - GitLens
     - Git Graph

   - **Install `Anaconda` package**

      Download link : https://www.anaconda.com/products/individual

   - **Run `Anaconda Navigator (Anaconda3)`**

      ~~~
         If you wand create own channel

           1) In the Anaconda Navigator, change from the `Home` tab to the `Environment` tab.
           2) And clieck `Create` you own channel

         Now you have own channel!
      ~~~

   - **Click `VS Code` Lanch button**

       ~~~
         If you created own channel
         
         change `Application on base(root)` to your own channel
      ~~~

## **[ How to change anaconda env `base` to `user-env` in VS Code ]**

   Open VS Code terminal

   ```
       $ conda activate `user-env-channel-name`
       ex) $ conda activate Estate-Plug_dev
   ```

## **[ How to add anaconda prompt at Visual Studio Code ]**

   1. Make Workspace

   2. Go to `Setting`

   3. Change tab `Workspace` at `User`

   4. Search Keyword `terminal shell`

   5. Find `Terminal > Integrated > Sheel: Windows`

   6. Click `setting.json에서 편집`

   7. Add Text like this (if you installed default setting anaconda package)
      ~~~
          "terminal.integrated.shellArgs.windows": [ 
                "/K", "C:\ProgramData\Anaconda3\Scripts\activate.bat C:\ProgramData\Anaconda3" 
          ]
      ~~~

      If you want to automatically set the prompt to the `user-env-channel` after running Visual Studio Code, modify as follows:
      ~~~
         "terminal.integrated.shellArgs.windows": [ 
                "/K", "C:\ProgramData\Anaconda3\Scripts\activate.bat C:\ProgramData\Anaconda3 & conda activate <user-env-channel" 
          ]
      ~~~

   8. Save `settings.json`

## **[ Set Run env at Visual Studio Code ]**

   1. Press `F1`

   2. insert `Python: Select Interpreter`

      than in `setting.json` you see like see
        ~~~
          ​"python.pythonPath": "C:\\Users\\<user>\\.conda\\envs\\Estate-Plug_dev\\python.exe"
        ~~~

## **[ Install Python Package ]**

   ~~~
      conda install --name <channel-name> <package-name>

      ex) conda install pandas
          conda install --name dev bs4
   ~~~

   What is diff `pip install` and `conda install`
   > So Conda is a packaging tool and installer that aims to do more than what pip does; handle library dependencies outside of the Python packages as well as the Python packages themselves. Conda also creates a virtual environment, like virtualenv does.
   >
   > src link : https://stackoverflow.com/questions/20994716/what-is-the-difference-between-pip-and-conda
   

## **[ Export your Pakage ]**

   ~~~
   $ conda env export > prj_env.yml
   ~~~

## **[ Import Pakage ]**

   ~~~
   $ conda env create -f prj_env.yml
   ~~~
