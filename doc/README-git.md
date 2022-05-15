### git config
Create repository in website github  
git config --global user.name "Your Name"  
git config --global user.email "youremail@yourdomain.com"   
git config --list / git config --list --show-origin  
  
### sync tree to local  
cd D:\workspace\git  
git clone https://github.com/textboy/mk_space.git / git clone --branch OEE-mike https://xxx  
cd mk_space  
git status  
  
### upload to github
git add . / git rm xxx  
git commit -m "init"  
(git defender --setup may be required)  
git push / git push origin mk-branch  
git diff --name-only  
visit https://github.com/textboy/mk_space  
  
### defender
git defender --setup (git config --global user.email "xxx@xxx.com”, https://codedefender.proserve.aws.dev/, VPN)  
git-defender --re_register  
  
### branch
Create a new branch based on the existing: git branch <new-branch> <base-branch>  
Switch to the branch: git checkout mk-branch  
  
### Flow
1) clone > (update) > add (add到缓存区) > commit (缓存区到本地库) > push (upload remote branch);  
2) checkout main > pull origin main (merge到本地master/main库，=fetch+merge) > review and approve.  
  
### Error
If update remote items directly will cause “failed to push”, and need to run “git pull” then “git push”.  
