#!/bin/bash

##############################################################################
# Show information (including parameter values) for all loaded modules.
# 
# This can be used as a diagnostic tool.
#
# Stick this script in /usr/local/bin or any other directory in path.

# This code is from: https://wiki.archlinux.org/index.php/kernel_modules
#
##############################################################################


function show_mod_parameter_info ()
{
	if tty -s <&1
	then
		green="\e[1;32m"
		yellow="\e[1;33m"
		cyan="\e[1;36m"
		reset="\e[0m"
	else
		green=
		yellow=
		cyan=
		reset=
	fi
	newline="
	"

	while read mod
	do
		md=/sys/module/$mod/parameters
		[[ ! -d $md ]] && continue
		d="$(modinfo -d $mod 2>/dev/null | tr "\n" "\t")"
		echo -en "$green$mod$reset"
		[[ ${#d} -gt 0 ]] && echo -n " - $d"
		echo
		pnames=()
		pdescs=()
		pvals=()
		pdesc=
		add_desc=false
		while IFS="$newline" read p
		do
			if [[ $p =~ ^[[:space:]] ]]
			then
				pdesc+="$newline    $p"
			else
				$add_desc && pdescs+=("$pdesc")
				pname="${p%%:*}"
				pnames+=("$pname")
				pdesc=("    ${p#*:}")
				pvals+=("$(cat $md/$pname 2>/dev/null)")
			fi
			add_desc=true
		done < <(modinfo -p $mod 2>/dev/null)
		$add_desc && pdescs+=("$pdesc")
		for ((i=0; i<${#pnames[@]}; i++))
		do
			printf "  $cyan%s$reset = $yellow%s$reset\n%s\n" \
				${pnames[i]} \
				"${pvals[i]}" \
				"${pdescs[i]}"
		done
		echo

	done < <(cut -d' ' -f1 /proc/modules | sort)
}

show_mod_parameter_info
exit 0
