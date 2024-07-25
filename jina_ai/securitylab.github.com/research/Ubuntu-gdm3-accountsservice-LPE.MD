How to get root on Ubuntu 20.04 by pretending nobody’s /home | GitHub Security Lab
===============
                    [skip to content](https://securitylab.github.com/research/Ubuntu-gdm3-accountsservice-LPE/#content)

[Back to GitHub.com](https://github.com/)

[Security Lab](https://securitylab.github.com/ "Security Lab")

[Research](https://securitylab.github.com/research/ "Research") [Advisories](https://securitylab.github.com/advisories/ "Advisories") [CodeQL Wall of Fame](https://securitylab.github.com/codeql-wall-of-fame/ "CodeQL Wall of Fame") [Get Involved](https://securitylab.github.com/get-involved/ "Get Involved") [Events](https://securitylab.github.com/events/ "Events")

[Home](https://securitylab.github.com/ "Home") [Research](https://securitylab.github.com/research/ "Research") [Advisories](https://securitylab.github.com/advisories/ "Advisories") [CodeQL Wall of Fame](https://securitylab.github.com/codeql-wall-of-fame/ "CodeQL Wall of Fame") [Get Involved](https://securitylab.github.com/get-involved/ "Get Involved") [Events](https://securitylab.github.com/events/ "Events")

November 10, 2020

How to get root on Ubuntu 20.04 by pretending nobody’s /home
============================================================

 [![Image 1](https://avatars.githubusercontent.com/u/4358136) Kevin Backhouse](https://github.com/kevinbackhouse)

I am a fan of Ubuntu, so I would like to help make it as secure as possible. I have recently spent quite a bit of time looking for security vulnerabilities in Ubuntu’s system services, and it has mostly been an exercise in frustration. I have found (and reported) a few issues, but the majority have been low severity. Ubuntu is open source, which means that many people have looked at the source code before me, and it seems like all the easy bugs have already been found. In other words, I don’t want this blog post to give you the impression that Ubuntu is full of trivial security bugs; that’s not been my impression so far.

This blog post is about an astonishingly straightforward way to escalate privileges on Ubuntu. With a few simple commands in the terminal, and a few mouse clicks, a standard user can create an administrator account for themselves. I have made a short [demo video](https://youtu.be/8IjTq7GBupw), to show how easy it is.

It’s unusual for a vulnerability on a modern operating system to be this easy to exploit. I have, on some occasions, written thousands of lines of code to exploit a vulnerability. Most modern exploits involve complicated trickery, like using a memory corruption vulnerability to forge fake objects in the heap, or replacing a file with a symlink with microsecond accuracy to exploit a [TOCTOU](https://en.wikipedia.org/wiki/Time-of-check_to_time-of-use) vulnerability. So these days it’s relatively rare to find a vulnerability that doesn’t require coding skills to exploit. I also think the vulnerability is easy to understand, even if you have no prior knowledge of how Ubuntu works or any security research experience.

_Disclaimer: For someone to exploit this vulnerability, they need access to the graphical desktop session of the system, so this issue affects desktop users only._

Exploitation steps
==================

Here is a description of the exploitation steps, as shown in the [demo video](https://youtu.be/8IjTq7GBupw).

First, open a terminal and create a [symlink](https://en.wikipedia.org/wiki/Symbolic_link) in your home directory:

```
ln -s /dev/zero .pam_environment
```

(If that doesn’t work because a file named `.pam_environment` already exists, then just temporarily rename the old file so that you can restore it later.)

Next, open “Region & Language” in the system settings and try to change the language. The dialog box will freeze, so just ignore it and go back to the terminal. At this point, a program named accounts-daemon is consuming 100% of a CPU core, so your computer may become sluggish and start to get hot.

In the terminal, delete the symlink. Otherwise you might lock yourself out of your own account!

```
rm .pam_environment
```

The next step is to send a SIGSTOP signal to accounts-daemon to stop it from thrashing that CPU core. But to do that, you first need to know accounts-daemon’s process identifier (PID). In the video, I do that by running [top](https://manpages.ubuntu.com/manpages/focal/en/man1/top.1.html), which is a utility for monitoring the running processes. Because accounts-daemon is stuck in an infinite loop, it quickly goes to the top of the list. Another way to find the PID is with the [pidof](https://manpages.ubuntu.com/manpages/focal/en/man8/pidof.8.html) utility:

```
$ pidof accounts-daemon
597
```

Armed with accounts-daemon’s PID, you can use [kill](https://manpages.ubuntu.com/manpages/focal/en/man1/kill.1.html) to send the SIGSTOP signal:

```
kill -SIGSTOP 597
```

Your computer can take a breather now.

_Here is the crucial step._ You’re going to log out of your account, but first you need to set a timer to reset accounts-daemon after you have logged out. Otherwise you’ll just be locked out and the exploit will fail. (Don’t worry if this happens: everything will be back to normal after a reboot.) This is how to set the timer:

```
nohup bash -c "sleep 30s; kill -SIGSEGV 597; kill -SIGCONT 597"
```

The [nohup](https://manpages.ubuntu.com/manpages/focal/en/man1/nohup.1posix.html) utility is a simple way to leave a script running after you have logged out. This command tells it to run a [bash](https://manpages.ubuntu.com/manpages/focal/en/man1/bash.1.html) script that does three things:

1.  Sleep for 30 seconds. (You just need to give yourself enough time to log out. I set it to 10 seconds for the video.)
2.  Send accounts-daemon a SIGSEGV signal, which will make it crash.
3.  Send accounts-daemon a SIGCONT signal to deactivate the SIGSTOP, which you sent earlier. The SIGSEGV won’t take effect until the SIGCONT is received.

Once completed, log out and wait a few seconds for the SIGSEGV to detonate. If the exploit is successful, then you will be presented with a series of dialog boxes which let you create a new user account. The new user account is an administrator account. (In the video, I run [id](https://manpages.ubuntu.com/manpages/focal/en/man1/id.1.html) to show that the new user is a member of the sudo group, which means that it has root privileges.)

![Image 2: Victory!](https://securitylab.github.com/assets/img/post-images/gdm3_accountsservice_victory.jpg)

How does it work?
=================

Stay with me! Even if you have no prior knowledge of how Ubuntu (or more specifically, GNOME) works, I reckon I can explain this vulnerability to you. There are actually two bugs involved. The first is in [accountsservice](https://launchpad.net/ubuntu/+source/accountsservice), which is a service that manages user accounts on the computer. The second is in [GNOME Display Manager](https://wiki.gnome.org/Projects/GDM) (gdm3), which, among other things, handles the login screen. I’ll explain each of these bugs separately below.

accountsservice denial of service (GHSL-2020-187, GHSL-2020-188 / CVE-2020-16126, CVE-2020-16127)
-------------------------------------------------------------------------------------------------

The accountsservice daemon (accounts-daemon) is a system service that manages user accounts on the machine. It can do things like create a new user account or change a user’s password, but it can also do less security-sensitive things like change a user’s icon or their preferred language. Daemons are programs that run in the background and do not have their own user interface. However, the systems settings dialog box can communicate with accounts-daemon via a message system known as [D-Bus](https://www.freedesktop.org/wiki/Software/dbus/).

![Image 3: System Settings: Users](https://securitylab.github.com/assets/img/post-images/gdm3_accountsservice_settings_users.png)

![Image 4: System Settings: Region & Language](https://securitylab.github.com/assets/img/post-images/gdm3_accountsservice_settings_region_language.png)

In the exploit, I use the systems settings dialog box to change the language. A standard user is allowed to change that setting on their own account - administrator privileges are not required. Under the hood, the systems services dialog box sends the [org.freedesktop.Accounts.User.SetLanguage](https://git.launchpad.net/ubuntu/+source/accountsservice/tree/data/org.freedesktop.Accounts.User.xml?h=ubuntu/focal-updates&id=e0347185d4c5554b026c13ccca691577c239afd5#n114) command to accounts-daemon, via D-Bus.

It turns out that Ubuntu uses a modified version of accountsservice that includes some [extra code](https://git.launchpad.net/ubuntu/+source/accountsservice/tree/debian/patches/0010-set-language.patch?h=ubuntu/focal-updates&id=e0347185d4c5554b026c13ccca691577c239afd5) that doesn’t exist in the upstream version maintained by [freedesktop](https://gitlab.freedesktop.org/accountsservice/accountsservice). Ubuntu’s patch adds a function named [`is_in_pam_environment`](https://git.launchpad.net/ubuntu/+source/accountsservice/tree/debian/patches/0010-set-language.patch?h=ubuntu/focal-updates&id=e0347185d4c5554b026c13ccca691577c239afd5#n367), which looks for a file named `.pam_environment` in the user’s home directory and reads it. The denial of service vulnerability works by making `.pam_environment` a [symlink](https://en.wikipedia.org/wiki/Symbolic_link) to `/dev/zero`. [`/dev/zero`](https://en.wikipedia.org/wiki//dev/zero) is a special file that doesn’t actually exist on disk. It is provided by the operating system and behaves like an infinitely long file in which every byte is zero. When `is_in_pam_environment` tries to read `.pam_environment`, it gets redirected to `/dev/zero` by the symlink, and then gets stuck in an infinite loop because `/dev/zero` is infinitely long.

There’s a second part to this bug. The exploit involves crashing accounts-daemon by sending it a SIGSEGV. Surely a standard user shouldn’t be allowed to crash a system service like that? They shouldn’t, but accounts-daemon inadvertently allows it by [dropping privileges](https://git.launchpad.net/ubuntu/+source/accountsservice/tree/debian/patches/0010-set-language.patch?h=ubuntu/focal-updates&id=e0347185d4c5554b026c13ccca691577c239afd5#n384) just before it starts reading the user’s `.pam_environment`. Dropping privileges means that the daemon temporarily forfeits its root privileges, adopting instead the lower privileges of the user. Ironically, that’s intended to be a security precaution, the goal of which is to protect the daemon from a malicious user who does something like symlinking their `.pam_environment` to `/etc/shadow`, which is a highly sensitive file that standard users aren’t allowed to read. Unfortunately, when done incorrectly, it also grants the user permission to send the daemon signals, which is why we’re able to send accounts-daemon a SIGSEGV.

gdm3 privilege escalation due to unresponsive accounts-daemon (GHSL-2020-202 / CVE-2020-16125)
----------------------------------------------------------------------------------------------

[GNOME Display Manager](https://wiki.gnome.org/Projects/GDM) (gdm3) is a fundamental component of Ubuntu’s user interface. It handles things like starting and stopping user sessions when they log in and out. It also manages the login screen.

![Image 5: gdm3 login screen](https://securitylab.github.com/assets/img/post-images/gdm3_accountsservice_login_screen.png)

Another thing handled by gdm3 is the initial setup of a new computer. When you install Ubuntu on a new computer, one of the first things that you need to do is create a user account. The initial user account needs to be an administrator so that you can continue setting up the machine, doing things like configuring the wifi and installing applications. Here is a screenshot of the initial setup screen (taken from the exploit video):

![Image 6: gnome-initial-setup](https://securitylab.github.com/assets/img/post-images/gdm3_accountsservice_gnome-initial-setup.png)

The dialog box that you see in the screenshot is a separate application, called [gnome-initial-setup](https://gitlab.gnome.org/GNOME/gnome-initial-setup). It is triggered by gdm3 when there are zero user accounts on the system, which is the expected scenario during the initial setup of a new computer. How does gdm3 check how many users there are on the system? You probably already guessed it: by asking accounts-daemon! So what happens if accounts-daemon is unresponsive? The relevant code is [here](https://gitlab.gnome.org/GNOME/gdm/-/blob/3.36.3/daemon/gdm-display.c#L513).

It uses D-Bus to ask accounts-daemon how many users there are, but since accounts-daemon is unresponsive, the D-Bus method call fails due to a timeout. (In my testing, the timeout took around 20 seconds.) Due to the timeout error, the code does not set the value of `priv->have_existing_user_accounts`. Unfortunately, the default value of `priv->have_existing_user_accounts` is `false`, not `true`, so now gdm3 thinks that there are zero user accounts and it launches `gnome-initial-setup`.

How did I find it?
==================

I have a confession to make: I found this bug completely by accident. This is the message that I sent to my colleagues at approximately 10pm BST on October 14:

```
I just got LPE by accident, but I am not quite sure how to reproduce it. 🤦
```

Here’s what happened: I had found a couple of denial-of-service vulnerabilities in accountsservice. I considered them low severity, but was writing them up for a vulnerability report to send to Ubuntu. Around 6pm, I stopped work and closed my laptop lid. Later in the evening, I opened the laptop lid and discovered that I was locked out of my account. I had been experimenting with the `.pam_environment` symlink and had forgotten to delete it before closing the lid. No big deal: I used Ctrl-Alt-F4 to open a [console](https://wiki.debian.org/Console), logged in (the console login was not affected by the accountsservice DOS), and killed accounts-daemon with a SIGSEGV. I didn’t need to use [sudo](https://manpages.ubuntu.com/manpages/focal/en/man8/sudo.8.html) due to the privilege dropping vulnerability. The next thing I knew, I was looking at the gnome-initial-setup dialog boxes, and was amazed to discover that I was able to create a new user with administrator privileges.

Unfortunately, when I tried to reproduce the same sequence of steps, I couldn’t get it to work again. I checked the system logs for clues, but there wasn’t much information because I didn’t have gdm’s debug messages enabled. The exploit that I have since developed requires the user to log out of their account, but I definitely didn’t do that on the evening of October 14. So it remains a mystery how I accidentally triggered the bug that evening.

Later that evening, I sent further messages to my (US-based) colleagues describing what had happened. Talking about the dialog boxes helped to jog my memory about something that I had noticed recently. Many of the system services that I have been looking at use [policykit](https://www.freedesktop.org/software/polkit/docs/latest/polkit.8.html) to check whether the client is authorized to request an action. I had noticed a file called [`gnome-initial-setup.pkla`](https://git.launchpad.net/ubuntu/+source/gnome-initial-setup/tree/debian/gnome-initial-setup.pkla?h=applied/ubuntu/focal-updates&id=2b3933f055f28d52b4dd39b9a405d528a9c20a29), which is a policykit configuration file that grants a user named gnome-initial-setup the ability to do a number of security-sensitive things, such as mounting filesystems and creating new user accounts. So I said to my colleagues: “I wonder if it has something to do with gnome-initial-setup,” and [Bas Alberts](https://github.com/anticomputer) almost immediately jumped in with a hypothesis that turned out to be right on the money: “You tricked gdm into launching gnome-initial-setup, I reckon, which maybe happens if a gdm session can’t verify that an account already exists.”

After that, it was just a matter of finding the code in gdm3 that triggers gnome-initial-setup and figuring out how to trigger it while accounts-daemon is unresponsive. I found that the relevant code is triggered when a user logs out.

And that’s the story of how the end of my workday was the start of an 0-day!

[](https://github.com/)

Product
-------

*   [Features](https://github.com/features)
*   [Security](https://github.com/security)
*   [Enterprise](https://github.com/enterprise)
*   [Customer stories](https://github.com/customer-stories?type=enterprise)
*   [Pricing](https://github.com/pricing)
*   [Resources](https://resources.github.com/)

Platform
--------

*   [Developer API](https://developer.github.com/)
*   [Partners](http://partner.github.com/)
*   [Atom](https://atom.io/)
*   [Electron](http://electron.atom.io/)
*   [GitHub Desktop](https://desktop.github.com/)

Support
-------

*   [Docs](https://docs.github.com/)
*   [Community Forum](https://github.community/)
*   [Professional Services](https://services.github.com/)
*   [Status](https://githubstatus.com/)
*   [Contact GitHub](https://support.github.com/)

Company
-------

*   [About](https://github.com/about)
*   [Blog](https://github.blog/)
*   [Careers](https://github.com/about/careers)
*   [Press](https://github.com/about/press)
*   [Shop](https://shop.github.com/)

*   [](https://twitter.com/github "GitHub on Twitter")
*   [](https://www.facebook.com/GitHub "GitHub on Facebook")
*   [](https://www.youtube.com/github "GitHub on YouTube")
*   [](https://www.linkedin.com/company/github "GitHub on Linkedin")
*   [](https://github.com/github "GitHub's organization")

*   © 2021 GitHub, Inc.
*   [Terms](https://docs.github.com/en/github/site-policy/github-terms-of-service)
*   [Privacy](https://docs.github.com/en/github/site-policy/github-privacy-statement)
*   [Cookie settings](https://securitylab.github.com/research/Ubuntu-gdm3-accountsservice-LPE/#)